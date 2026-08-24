package alert

import (
	"context"
	"database/sql"
	"errors"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
)

type Repository struct{ DB *sqlite.DB }

func NewRepository(db *sqlite.DB) *Repository { return &Repository{DB: db} }
func (r *Repository) Find(ctx context.Context, id string) (domain.Alert, error) {
	var a domain.Alert
	var next, del string
	err := r.DB.QueryRowContext(ctx, `SELECT id,storm_id,COALESCE(order_id,''),channel,status,attempts,next_attempt_at,COALESCE(last_error,''),COALESCE(delivered_at,'') FROM alerts WHERE id=?`, id).Scan(&a.ID, &a.StormID, &a.OrderID, &a.Channel, &a.Status, &a.Attempts, &next, &a.LastError, &del)
	if errors.Is(err, sql.ErrNoRows) {
		return a, domain.ErrNotFound
	}
	a.NextAttemptAt = sqlite.TimeOrZero(next)
	a.DeliveredAt = sqlite.TimeOrZero(del)
	return a, err
}
func (r *Repository) CountPending(ctx context.Context, stormID string) (int, error) {
	var n int
	err := r.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM alerts WHERE storm_id=? AND status='pending'`, stormID).Scan(&n)
	return n, err
}
func (r *Repository) Dead(ctx context.Context, stormID string) ([]domain.Alert, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id,storm_id,COALESCE(order_id,''),channel,status,attempts,next_attempt_at,COALESCE(last_error,''),COALESCE(delivered_at,'') FROM alerts WHERE storm_id=? AND status='dead' ORDER BY id`, stormID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Alert
	for rows.Next() {
		var a domain.Alert
		var next, del string
		if err := rows.Scan(&a.ID, &a.StormID, &a.OrderID, &a.Channel, &a.Status, &a.Attempts, &next, &a.LastError, &del); err != nil {
			return nil, err
		}
		a.NextAttemptAt = sqlite.TimeOrZero(next)
		a.DeliveredAt = sqlite.TimeOrZero(del)
		out = append(out, a)
	}
	return out, rows.Err()
}
