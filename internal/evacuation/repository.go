package evacuation

import (
	"context"
	"database/sql"
	"errors"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
)

type Repository struct{ DB *sqlite.DB }

func NewRepository(db *sqlite.DB) *Repository { return &Repository{DB: db} }
func (r *Repository) Orders(ctx context.Context, stormID string) ([]domain.EvacuationOrder, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id,storm_id,zone_id,status,version,COALESCE(published_at,''),COALESCE(cancelled_at,'') FROM evacuation_orders WHERE storm_id=? ORDER BY id`, stormID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.EvacuationOrder
	for rows.Next() {
		var x domain.EvacuationOrder
		var st, pub, cancel string
		if err := rows.Scan(&x.ID, &x.StormID, &x.ZoneID, &st, &x.Version, &pub, &cancel); err != nil {
			return nil, err
		}
		x.Status = domain.OrderStatus(st)
		x.PublishedAt = sqlite.TimeOrZero(pub)
		x.CancelledAt = sqlite.TimeOrZero(cancel)
		out = append(out, x)
	}
	return out, rows.Err()
}
func (r *Repository) Households(ctx context.Context, zoneID, status string) ([]domain.Household, error) {
	query := `SELECT id,zone_id,head_name,people,status,COALESCE(shelter_id,''),COALESCE(confirmed_at,'') FROM households WHERE zone_id=?`
	args := []any{zoneID}
	if status != "" {
		query += ` AND status=?`
		args = append(args, status)
	}
	query += ` ORDER BY head_name,id`
	rows, err := r.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Household
	for rows.Next() {
		var x domain.Household
		var st, confirmed string
		if err := rows.Scan(&x.ID, &x.ZoneID, &x.HeadName, &x.People, &st, &x.ShelterID, &confirmed); err != nil {
			return nil, err
		}
		x.Status = domain.HouseholdStatus(st)
		x.ConfirmedAt = sqlite.TimeOrZero(confirmed)
		out = append(out, x)
	}
	return out, rows.Err()
}
func (r *Repository) Order(ctx context.Context, id string) (domain.EvacuationOrder, error) {
	var x domain.EvacuationOrder
	var st, pub, cancel string
	err := r.DB.QueryRowContext(ctx, `SELECT id,storm_id,zone_id,status,version,COALESCE(published_at,''),COALESCE(cancelled_at,'') FROM evacuation_orders WHERE id=?`, id).Scan(&x.ID, &x.StormID, &x.ZoneID, &st, &x.Version, &pub, &cancel)
	if errors.Is(err, sql.ErrNoRows) {
		return x, domain.ErrNotFound
	}
	x.Status = domain.OrderStatus(st)
	x.PublishedAt = sqlite.TimeOrZero(pub)
	x.CancelledAt = sqlite.TimeOrZero(cancel)
	return x, err
}
func (r *Repository) CountUnconfirmed(ctx context.Context, zoneID string) (int, error) {
	var n int
	err := r.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM households WHERE zone_id=? AND status!='safe'`, zoneID).Scan(&n)
	return n, err
}
