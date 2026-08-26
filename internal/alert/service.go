package alert

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/audit"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/outbox"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
	"time"
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
func (s *Service) EnqueueOrderAlert(ctx context.Context, stormID, orderID, org, actor, requestID, channel string) (domain.Alert, error) {
	if stormID == "" || orderID == "" {
		return domain.Alert{}, domain.ErrValidation
	}
	if channel == "" {
		channel = "webhook"
	}
	item := domain.Alert{ID: domain.NewID("alert"), StormID: stormID, OrderID: orderID, Channel: channel, Status: "pending", NextAttemptAt: s.Now().UTC()}
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM evacuation_orders WHERE id=? AND storm_id=?`, orderID, stormID).Scan(&status); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if status != string(domain.OrderPublished) {
			return domain.ErrInvalidState
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO alerts(id,storm_id,order_id,channel,status,attempts,next_attempt_at) VALUES(?,?,?,?,?,0,?)`, item.ID, stormID, orderID, channel, item.Status, sqlite.FormatTime(item.NextAttemptAt)); err != nil {
			return err
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, org, actor, "evacuation_order", orderID, "alert.enqueue", "success", requestID, item); err != nil {
				return err
			}
		}
		if s.Outbox != nil {
			return s.Outbox.Enqueue(ctx, tx, "alert", item.ID, "alert.requested", map[string]string{"storm_id": stormID, "order_id": orderID, "channel": channel})
		}
		return nil
	})
	if sqlite.IsConstraint(err) {
		return domain.Alert{}, domain.ErrConflict
	}
	return item, err
}
func (s *Service) MarkDelivered(ctx context.Context, id string) error {
	return s.DB.Tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE alerts SET status='delivered',delivered_at=?,last_error=NULL WHERE id=? AND status='pending'`, sqlite.FormatTime(s.Now()), id)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return domain.ErrConflict
		}
		return nil
	})
}
func (s *Service) MarkFailed(ctx context.Context, id string, cause error, max int) error {
	if cause == nil {
		cause = fmt.Errorf("unknown alert failure")
	}
	if max <= 0 {
		max = 5
	}
	return s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var attempts int
		if err := tx.QueryRowContext(ctx, `SELECT attempts FROM alerts WHERE id=? AND status='pending'`, id).Scan(&attempts); err != nil {
			return err
		}
		attempts++
		status := "pending"
		if attempts >= max {
			status = "dead"
		}
		next := s.Now().UTC().Add(time.Duration(1<<min(attempts, 6)) * time.Second)
		_, err := tx.ExecContext(ctx, `UPDATE alerts SET status=?,attempts=?,next_attempt_at=?,last_error=? WHERE id=? AND status='pending'`, status, attempts, sqlite.FormatTime(next), cause.Error(), id)
		return err
	})
}
func (s *Service) Due(ctx context.Context, limit int) ([]domain.Alert, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT id,storm_id,COALESCE(order_id,''),channel,status,attempts,next_attempt_at,COALESCE(last_error,''),COALESCE(delivered_at,'') FROM alerts WHERE status='pending' AND next_attempt_at<=? ORDER BY next_attempt_at,id LIMIT ?`, sqlite.FormatTime(s.Now()), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.Alert
	for rows.Next() {
		var a domain.Alert
		var next, del string
		if err := rows.Scan(&a.ID, &a.StormID, &a.OrderID, &a.Channel, &a.Status, &a.Attempts, &next, &a.LastError, &del); err != nil {
			return nil, err
		}
		a.NextAttemptAt = sqlite.TimeOrZero(next)
		a.DeliveredAt = sqlite.TimeOrZero(del)
		result = append(result, a)
	}
	return result, rows.Err()
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
