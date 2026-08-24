package shelter

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/audit"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/outbox"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
)

type Service struct {
	DB     *sqlite.DB
	Audit  *audit.Service
	Outbox *outbox.Service
	Now    func() time.Time
}

func New(db *sqlite.DB, a *audit.Service, o *outbox.Service) *Service {
	return &Service{DB: db, Audit: a, Outbox: o, Now: time.Now}
}

type CreateInput struct {
	OrganizationID, Name, Address string
	Capacity                      int
	ActorID, RequestID            string
}

func (s *Service) Create(ctx context.Context, in CreateInput) (domain.Shelter, error) {
	if in.Name == "" || in.Address == "" || in.Capacity <= 0 {
		return domain.Shelter{}, domain.ErrValidation
	}
	item := domain.Shelter{ID: domain.NewID("shelter"), OrganizationID: in.OrganizationID, Name: in.Name, Address: in.Address, Status: domain.ShelterOpen, Capacity: in.Capacity, Version: 1}
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO shelters(id,organization_id,name,address,status,capacity,reserved,version) VALUES(?,?,?,?,?,?,0,1)`, item.ID, item.OrganizationID, item.Name, item.Address, item.Status, item.Capacity); err != nil {
			return err
		}
		if s.Audit != nil {
			return s.Audit.Record(ctx, tx, in.OrganizationID, in.ActorID, "shelter", item.ID, "create", "success", in.RequestID, in)
		}
		return nil
	})
	if sqlite.IsConstraint(err) {
		return domain.Shelter{}, domain.ErrConflict
	}
	return item, err
}
func (s *Service) Get(ctx context.Context, id string) (domain.Shelter, error) {
	var item domain.Shelter
	var status string
	err := s.DB.QueryRowContext(ctx, `SELECT id,organization_id,name,address,status,capacity,reserved,version FROM shelters WHERE id=?`, id).Scan(&item.ID, &item.OrganizationID, &item.Name, &item.Address, &status, &item.Capacity, &item.Reserved, &item.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return item, domain.ErrNotFound
	}
	item.Status = domain.ShelterStatus(status)
	return item, err
}
func (s *Service) Reserve(ctx context.Context, shelterID, householdID string, people int, ttl time.Duration, org, actor, requestID string) (domain.Reservation, error) {
	if shelterID == "" || householdID == "" || people <= 0 {
		return domain.Reservation{}, domain.ErrValidation
	}
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	item := domain.Reservation{ID: domain.NewID("reserve"), ShelterID: shelterID, HouseholdID: householdID, People: people, Status: "active", ExpiresAt: s.Now().UTC().Add(ttl), CreatedAt: s.Now().UTC()}
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var status string
		var capacity, reserved, version int
		if err := tx.QueryRowContext(ctx, `SELECT status,capacity,reserved,version FROM shelters WHERE id=?`, shelterID).Scan(&status, &capacity, &reserved, &version); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if status != string(domain.ShelterOpen) {
			return domain.ErrInvalidState
		}
		if capacity-reserved < people {
			return fmt.Errorf("%w: shelter capacity", domain.ErrConflict)
		}
		res, err := tx.ExecContext(ctx, `UPDATE shelters SET reserved=reserved+?,version=version+1 WHERE id=? AND status='open' AND version=? AND capacity-reserved>=?`, people, shelterID, version, people)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return domain.ErrConflict
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO shelter_reservations(id,shelter_id,household_id,people,status,expires_at,created_at) VALUES(?,?,?,?,?,?,?)`, item.ID, item.ShelterID, item.HouseholdID, item.People, item.Status, sqlite.FormatTime(item.ExpiresAt), sqlite.FormatTime(item.CreatedAt)); err != nil {
			return err
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, org, actor, "shelter", shelterID, "reserve", "success", requestID, item); err != nil {
				return err
			}
		}
		if s.Outbox != nil {
			return s.Outbox.Enqueue(ctx, tx, "shelter_reservation", item.ID, "shelter.reserved", item)
		}
		return nil
	})
	if sqlite.IsConstraint(err) {
		return domain.Reservation{}, domain.ErrConflict
	}
	return item, err
}
func (s *Service) ReleaseExpired(ctx context.Context, now time.Time) (int, error) {
	var released int
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT id,shelter_id,people FROM shelter_reservations WHERE status='active' AND expires_at<?`, sqlite.FormatTime(now))
		if err != nil {
			return err
		}
		defer rows.Close()
		type expired struct {
			id, shelter string
			people      int
		}
		var items []expired
		for rows.Next() {
			var x expired
			if err := rows.Scan(&x.id, &x.shelter, &x.people); err != nil {
				return err
			}
			items = append(items, x)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, x := range items {
			if _, err := tx.ExecContext(ctx, `UPDATE shelter_reservations SET status='expired' WHERE id=? AND status='active'`, x.id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE shelters SET reserved=reserved-?,version=version+1 WHERE id=? AND reserved>=?`, x.people, x.shelter, x.people); err != nil {
				return err
			}
			released++
		}
		return nil
	})
	return released, err
}
func (s *Service) List(ctx context.Context, limit int) ([]domain.Shelter, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT id,organization_id,name,address,status,capacity,reserved,version FROM shelters ORDER BY name,id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.Shelter
	for rows.Next() {
		var x domain.Shelter
		var st string
		if err := rows.Scan(&x.ID, &x.OrganizationID, &x.Name, &x.Address, &st, &x.Capacity, &x.Reserved, &x.Version); err != nil {
			return nil, err
		}
		x.Status = domain.ShelterStatus(st)
		result = append(result, x)
	}
	return result, rows.Err()
}
