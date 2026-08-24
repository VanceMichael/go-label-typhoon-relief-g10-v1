package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
)

type Service struct {
	DB  *sqlite.DB
	Now func() time.Time
}

func New(db *sqlite.DB) *Service { return &Service{DB: db, Now: time.Now} }
func (s *Service) Enqueue(ctx context.Context, tx *sql.Tx, aggregateType, aggregateID, eventType string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("outbox payload: %w", err)
	}
	now := s.Now().UTC()
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,aggregate_type,aggregate_id,event_type,payload,status,attempts,available_at,created_at) VALUES(?,?,?,?,?,'pending',0,?,?)`, domain.NewID("evt"), aggregateType, aggregateID, eventType, string(body), sqlite.FormatTime(now), sqlite.FormatTime(now))
	return err
}
func (s *Service) Claim(ctx context.Context, owner string, lease time.Duration, now time.Time) (domain.OutboxEvent, error) {
	var ev domain.OutboxEvent
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var available, until, created string
		row := tx.QueryRowContext(ctx, `SELECT id,aggregate_type,aggregate_id,event_type,payload,status,attempts,available_at,COALESCE(lease_owner,''),COALESCE(lease_until,''),COALESCE(last_error,''),created_at FROM outbox_events WHERE status='pending' AND available_at<=? AND (lease_until IS NULL OR lease_until<?) ORDER BY created_at,id LIMIT 1`, sqlite.FormatTime(now), sqlite.FormatTime(now))
		if err := row.Scan(&ev.ID, &ev.AggregateType, &ev.AggregateID, &ev.EventType, &ev.Payload, &ev.Status, &ev.Attempts, &available, &ev.LeaseOwner, &until, &ev.LastError, &created); err != nil {
			return err
		}
		ev.AvailableAt = sqlite.TimeOrZero(available)
		ev.LeaseUntil = now.Add(lease)
		ev.LeaseOwner = owner
		ev.CreatedAt = sqlite.TimeOrZero(created)
		res, err := tx.ExecContext(ctx, `UPDATE outbox_events SET lease_owner=?,lease_until=?,attempts=attempts+1 WHERE id=? AND status='pending'`, owner, sqlite.FormatTime(ev.LeaseUntil), ev.ID)
		if err != nil {
			return fmt.Errorf("claim outbox event: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return domain.ErrLeaseLost
		}
		ev.Attempts++
		return nil
	})
	if err != nil {
		return domain.OutboxEvent{}, err
	}
	return ev, nil
}
func (s *Service) Ack(ctx context.Context, id, owner string) error {
	res, err := s.DB.ExecContext(ctx, `UPDATE outbox_events SET status='delivered',lease_owner=NULL,lease_until=NULL WHERE id=? AND status='pending' AND lease_owner=?`, id, owner)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return domain.ErrLeaseLost
	}
	return nil
}
func (s *Service) Fail(ctx context.Context, id, owner string, next time.Time, cause error) error {
	if cause == nil {
		cause = fmt.Errorf("unknown delivery failure")
	}
	res, err := s.DB.ExecContext(ctx, `UPDATE outbox_events SET status='pending',lease_owner=NULL,lease_until=NULL,available_at=?,last_error=? WHERE id=? AND status='pending' AND lease_owner=?`, sqlite.FormatTime(next), cause.Error(), id, owner)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return domain.ErrLeaseLost
	}
	return nil
}
func (s *Service) Dead(ctx context.Context, id, owner string, cause error) error {
	if cause == nil {
		cause = fmt.Errorf("unknown delivery failure")
	}
	res, err := s.DB.ExecContext(ctx, `UPDATE outbox_events SET status='dead',lease_owner=NULL,lease_until=NULL,last_error=? WHERE id=? AND status='pending' AND lease_owner=?`, cause.Error(), id, owner)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return domain.ErrLeaseLost
	}
	return nil
}
