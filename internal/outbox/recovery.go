package outbox

import (
	"context"
	"errors"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
	"time"
)

type Recovery struct {
	DB  *sqlite.DB
	Now func() time.Time
}

func NewRecovery(db *sqlite.DB) *Recovery { return &Recovery{DB: db, Now: time.Now} }
func (r *Recovery) Expired(ctx context.Context) ([]domain.OutboxEvent, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id,aggregate_type,aggregate_id,event_type,payload,status,attempts,available_at,COALESCE(lease_owner,''),COALESCE(lease_until,''),COALESCE(last_error,''),created_at FROM outbox_events WHERE status='pending' AND lease_until<? ORDER BY lease_until,id`, sqlite.FormatTime(r.Now()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.OutboxEvent
	for rows.Next() {
		var e domain.OutboxEvent
		var available, lease, created string
		if err := rows.Scan(&e.ID, &e.AggregateType, &e.AggregateID, &e.EventType, &e.Payload, &e.Status, &e.Attempts, &available, &e.LeaseOwner, &lease, &e.LastError, &created); err != nil {
			return nil, err
		}
		e.AvailableAt = sqlite.TimeOrZero(available)
		e.LeaseUntil = sqlite.TimeOrZero(lease)
		e.CreatedAt = sqlite.TimeOrZero(created)
		out = append(out, e)
	}
	return out, rows.Err()
}
func (r *Recovery) Requeue(ctx context.Context, id string) error {
	res, err := r.DB.ExecContext(ctx, `UPDATE outbox_events SET lease_owner=NULL,lease_until=NULL,available_at=?,last_error=NULL WHERE id=? AND status='pending'`, sqlite.FormatTime(r.Now()), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return domain.ErrNotFound
	}
	return nil
}
func (r *Recovery) Terminal(ctx context.Context, id string, cause error) error {
	if cause == nil {
		cause = errors.New("worker failed")
	}
	res, err := r.DB.ExecContext(ctx, `UPDATE outbox_events SET status='dead',lease_owner=NULL,lease_until=NULL,last_error=? WHERE id=? AND status='pending'`, cause.Error(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return domain.ErrNotFound
	}
	return nil
}
func (r *Recovery) Purge(ctx context.Context, before time.Time) (int64, error) {
	res, err := r.DB.ExecContext(ctx, `DELETE FROM outbox_events WHERE status='dead' AND created_at<?`, sqlite.FormatTime(before))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
