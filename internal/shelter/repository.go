package shelter

import (
	"context"
	"database/sql"
	"errors"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
)

type Repository struct{ DB *sqlite.DB }

func NewRepository(db *sqlite.DB) *Repository { return &Repository{DB: db} }
func (r *Repository) Reservations(ctx context.Context, shelterID string) ([]domain.Reservation, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id,shelter_id,household_id,people,status,expires_at,created_at FROM shelter_reservations WHERE shelter_id=? ORDER BY created_at,id`, shelterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Reservation
	for rows.Next() {
		var x domain.Reservation
		var exp, created string
		if err := rows.Scan(&x.ID, &x.ShelterID, &x.HouseholdID, &x.People, &x.Status, &exp, &created); err != nil {
			return nil, err
		}
		x.ExpiresAt = sqlite.TimeOrZero(exp)
		x.CreatedAt = sqlite.TimeOrZero(created)
		out = append(out, x)
	}
	return out, rows.Err()
}
func (r *Repository) Available(ctx context.Context, limit int) ([]domain.Shelter, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := r.DB.QueryContext(ctx, `SELECT id,organization_id,name,address,status,capacity,reserved,version FROM shelters WHERE status='open' AND capacity>reserved ORDER BY (capacity-reserved) DESC,name LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Shelter
	for rows.Next() {
		var x domain.Shelter
		var st string
		if err := rows.Scan(&x.ID, &x.OrganizationID, &x.Name, &x.Address, &st, &x.Capacity, &x.Reserved, &x.Version); err != nil {
			return nil, err
		}
		x.Status = domain.ShelterStatus(st)
		out = append(out, x)
	}
	return out, rows.Err()
}
func (r *Repository) Reservation(ctx context.Context, id string) (domain.Reservation, error) {
	var x domain.Reservation
	var exp, created string
	err := r.DB.QueryRowContext(ctx, `SELECT id,shelter_id,household_id,people,status,expires_at,created_at FROM shelter_reservations WHERE id=?`, id).Scan(&x.ID, &x.ShelterID, &x.HouseholdID, &x.People, &x.Status, &exp, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return x, domain.ErrNotFound
	}
	x.ExpiresAt = sqlite.TimeOrZero(exp)
	x.CreatedAt = sqlite.TimeOrZero(created)
	return x, err
}
func (r *Repository) Capacity(ctx context.Context, id string) (int, int, error) {
	var capacity, reserved int
	err := r.DB.QueryRowContext(ctx, `SELECT capacity,reserved FROM shelters WHERE id=?`, id).Scan(&capacity, &reserved)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, domain.ErrNotFound
	}
	return capacity, reserved, err
}
