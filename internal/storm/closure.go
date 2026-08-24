package storm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
)

func (s *Service) Close(ctx context.Context, id, actor, requestID string) error {
	return s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var status string
		var version int
		var openOrders, openTasks, inTransit int
		if err := tx.QueryRowContext(ctx, `SELECT status,version FROM storm_events WHERE id=?`, id).Scan(&status, &version); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if status != string(domain.StormActive) {
			return domain.ErrInvalidState
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM evacuation_orders WHERE storm_id=? AND status='published'`, id).Scan(&openOrders); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dispatch_tasks WHERE storm_id=? AND status IN ('claimed')`, id).Scan(&openTasks); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return domain.ErrNotFound
			}
			return fmt.Errorf("close dispatch gate: %w", err)
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM supply_movements WHERE storm_id=? AND status='in_transit'`, id).Scan(&inTransit); err != nil {
			return err
		}
		if openOrders > 0 || openTasks > 0 || inTransit > 0 {
			return fmt.Errorf("%w: orders=%d tasks=%d movements=%d", domain.ErrConflict, openOrders, openTasks, inTransit)
		}
		res, err := tx.ExecContext(ctx, `UPDATE storm_events SET status='closed',version=version+1,closed_at=CURRENT_TIMESTAMP WHERE id=? AND status='active' AND version=?`, id, version)
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
func (s *Service) CloseReadiness(ctx context.Context, id string) (bool, error) {
	var status string
	var n int
	if err := s.DB.QueryRowContext(ctx, `SELECT status FROM storm_events WHERE id=?`, id).Scan(&status); err != nil {
		return false, err
	}
	if status != string(domain.StormActive) {
		return false, nil
	}
	queries := []string{`SELECT COUNT(*) FROM evacuation_orders WHERE storm_id=? AND status='published'`, `SELECT COUNT(*) FROM dispatch_tasks WHERE storm_id=? AND status IN ('queued','claimed')`, `SELECT COUNT(*) FROM supply_movements WHERE storm_id=? AND status='in_transit'`}
	for _, q := range queries {
		if err := s.DB.QueryRowContext(ctx, q, id).Scan(&n); err != nil {
			return false, err
		}
		if n > 0 {
			return false, nil
		}
	}
	return true, nil
}
