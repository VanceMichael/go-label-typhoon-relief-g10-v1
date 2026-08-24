package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
)

type Service struct {
	DB  *sqlite.DB
	TTL time.Duration
}
type LoginResult struct {
	Token     string
	ExpiresAt time.Time
	User      domain.User
}

func New(db *sqlite.DB, ttl time.Duration) *Service {
	if ttl <= 0 {
		ttl = 8 * time.Hour
	}
	return &Service{DB: db, TTL: ttl}
}
func hash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func Bootstrap(ctx context.Context, db *sqlite.DB, email, password string) error {
	now := time.Now().UTC()
	return db.Tx(ctx, func(tx *sql.Tx) error {
		var org string
		if err := tx.QueryRowContext(ctx, `SELECT id FROM organizations LIMIT 1`).Scan(&org); errors.Is(err, sql.ErrNoRows) {
			org = domain.NewID("org")
			if _, err := tx.ExecContext(ctx, `INSERT INTO organizations(id,name,kind,created_at) VALUES(?,?,?,?)`, org, "Emergency Command", "command", sqlite.FormatTime(now)); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		var n int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE email=?`, email).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			_, err := tx.ExecContext(ctx, `INSERT INTO users(id,organization_id,email,password_hash,role,created_at) VALUES(?,?,?,?,?,?)`, domain.NewID("usr"), org, email, hash(password), string(domain.RoleCommander), sqlite.FormatTime(now))
			return err
		}
		return nil
	})
}
func (s *Service) Login(ctx context.Context, email, password string) (LoginResult, error) {
	var u domain.User
	var active int
	var stored string
	var created string
	err := s.DB.QueryRowContext(ctx, `SELECT id,organization_id,password_hash,role,active,created_at FROM users WHERE email=?`, email).Scan(&u.ID, &u.OrganizationID, &stored, &u.Role, &active, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return LoginResult{}, domain.ErrUnauthorized
	}
	if err != nil {
		return LoginResult{}, err
	}
	if subtle.ConstantTimeCompare([]byte(stored), []byte(hash(password))) != 1 || active == 0 {
		return LoginResult{}, domain.ErrUnauthorized
	}
	u.Email = email
	u.Active = true
	u.CreatedAt = sqlite.TimeOrZero(created)
	token := domain.NewID("tok")
	expires := time.Now().UTC().Add(s.TTL)
	err = s.DB.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO sessions(id,user_id,token_hash,expires_at,created_at) VALUES(?,?,?,?,?)`, domain.NewID("ses"), u.ID, hash(token), sqlite.FormatTime(expires), sqlite.FormatTime(time.Now()))
		return err
	})
	if err != nil {
		return LoginResult{}, err
	}
	return LoginResult{Token: token, ExpiresAt: expires, User: u}, nil
}
func (s *Service) Authenticate(ctx context.Context, token string) (domain.User, error) {
	if token == "" {
		return domain.User{}, domain.ErrUnauthorized
	}
	var u domain.User
	var active int
	var expires, revoked, created string
	err := s.DB.QueryRowContext(ctx, `SELECT u.id,u.organization_id,u.email,u.role,u.active,u.created_at,s.expires_at,COALESCE(s.revoked_at,'') FROM users u JOIN sessions s ON s.user_id=u.id WHERE s.token_hash=?`, hash(token)).Scan(&u.ID, &u.OrganizationID, &u.Email, &u.Role, &active, &created, &expires, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.User{}, domain.ErrUnauthorized
	}
	if err != nil {
		return domain.User{}, err
	}
	if active == 0 || revoked != "" || !sqlite.TimeOrZero(expires).After(time.Now()) {
		return domain.User{}, domain.ErrUnauthorized
	}
	u.Active = true
	u.CreatedAt = sqlite.TimeOrZero(created)
	return u, nil
}
func (s *Service) Logout(ctx context.Context, u domain.User, token, requestID string) error {
	if token == "" || u.ID == "" {
		return domain.ErrUnauthorized
	}
	return s.DB.Tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE sessions SET revoked_at=? WHERE token_hash=? AND revoked_at IS NULL`, sqlite.FormatTime(time.Now().UTC()), hash(token))
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return domain.ErrUnauthorized
		}
		return nil
	})
}
func (s *Service) Expire(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE sessions SET revoked_at=expires_at WHERE revoked_at IS NULL AND expires_at<?`, sqlite.FormatTime(time.Now().UTC()))
	return err
}
func RequireRole(u domain.User, action string) error {
	if !domain.UserRole(u.Role).Can(action) {
		return fmt.Errorf("%w: %s", domain.ErrForbidden, action)
	}
	return nil
}
