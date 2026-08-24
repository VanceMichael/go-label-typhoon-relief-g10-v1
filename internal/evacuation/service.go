package evacuation

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
func (s *Service) Create(ctx context.Context, stormID, zoneID, org, actor, requestID string) (domain.EvacuationOrder, error) {
	if stormID == "" || zoneID == "" {
		return domain.EvacuationOrder{}, domain.ErrValidation
	}
	item := domain.EvacuationOrder{ID: domain.NewID("order"), StormID: stormID, ZoneID: zoneID, Status: domain.OrderDraft, Version: 1}
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var confirmed int
		if err := tx.QueryRowContext(ctx, `SELECT confirmed FROM risk_zones WHERE id=? AND storm_id=?`, zoneID, stormID).Scan(&confirmed); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if confirmed != 1 {
			return fmt.Errorf("%w: zone not confirmed", domain.ErrInvalidState)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO evacuation_orders(id,storm_id,zone_id,status,version) VALUES(?,?,?,?,1)`, item.ID, stormID, zoneID, item.Status); err != nil {
			return err
		}
		if s.Audit != nil {
			return s.Audit.Record(ctx, tx, org, actor, "evacuation_order", item.ID, "create", "success", requestID, item)
		}
		return nil
	})
	if sqlite.IsConstraint(err) {
		return domain.EvacuationOrder{}, domain.ErrConflict
	}
	return item, err
}
func (s *Service) Publish(ctx context.Context, id, org, actor, requestID string) error {
	return s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var storm, zone, status string
		var version int
		if err := tx.QueryRowContext(ctx, `SELECT storm_id,zone_id,status,version FROM evacuation_orders WHERE id=?`, id).Scan(&storm, &zone, &status, &version); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if domain.OrderStatus(status) != domain.OrderDraft {
			return domain.ErrInvalidState
		}
		now := s.Now().UTC()
		res, err := tx.ExecContext(ctx, `UPDATE evacuation_orders SET status='published',version=version+1,published_at=? WHERE id=? AND status='draft' AND version=?`, sqlite.FormatTime(now), id, version)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return domain.ErrConflict
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, org, actor, "evacuation_order", id, "publish", "success", requestID, map[string]string{"storm": storm, "zone": zone}); err != nil {
				return err
			}
		}
		if s.Outbox != nil {
			return s.Outbox.Enqueue(ctx, tx, "evacuation_order", id, "evacuation.published", map[string]string{"storm_id": storm, "zone_id": zone})
		}
		return nil
	})
}
func (s *Service) Cancel(ctx context.Context, id, org, actor, requestID string) error {
	return s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var status string
		var version int
		if err := tx.QueryRowContext(ctx, `SELECT status,version FROM evacuation_orders WHERE id=?`, id).Scan(&status, &version); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if domain.OrderStatus(status) != domain.OrderPublished {
			return domain.ErrInvalidState
		}
		var confirmed int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM households h JOIN risk_zones z ON z.id=h.zone_id JOIN evacuation_orders o ON o.zone_id=z.id WHERE o.id=? AND h.status='confirmed'`, id).Scan(&confirmed); err != nil {
			return err
		}
		if confirmed > 0 {
			return fmt.Errorf("%w: households confirmed", domain.ErrConflict)
		}
		now := s.Now().UTC()
		res, err := tx.ExecContext(ctx, `UPDATE evacuation_orders SET status='cancelled',version=version+1,cancelled_at=? WHERE id=? AND status='published' AND version=?`, sqlite.FormatTime(now), id, version)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return domain.ErrConflict
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, org, actor, "evacuation_order", id, "cancel", "success", requestID, map[string]int{"confirmed": confirmed}); err != nil {
				if sqlite.IsConstraint(err) {
					return nil
				}
				return nil
			}
		}
		return nil
	})
}
func (s *Service) ConfirmHousehold(ctx context.Context, householdID, shelterID, org, actor, requestID string) error {
	return s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var zone, orderStatus, stored string
		var people int
		if err := tx.QueryRowContext(ctx, `SELECT zone_id,status FROM households WHERE id=?`, householdID).Scan(&zone, &stored); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if stored != string(domain.HouseholdPending) {
			return domain.ErrInvalidState
		}
		if err := tx.QueryRowContext(ctx, `SELECT status FROM evacuation_orders WHERE zone_id=? ORDER BY rowid DESC LIMIT 1`, zone).Scan(&orderStatus); err != nil {
			return err
		}
		if orderStatus != string(domain.OrderPublished) {
			return domain.ErrInvalidState
		}
		if err := tx.QueryRowContext(ctx, `SELECT people FROM households WHERE id=?`, householdID).Scan(&people); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE households SET status='confirmed',shelter_id=?,confirmed_at=? WHERE id=? AND status='pending'`, shelterID, sqlite.FormatTime(s.Now()), householdID)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return domain.ErrConflict
		}
		if s.Audit != nil {
			return s.Audit.Record(ctx, tx, org, actor, "household", householdID, "confirm", "success", requestID, map[string]any{"people": people})
		}
		return nil
	})
}
func (s *Service) AddHousehold(ctx context.Context, zoneID, name string, people int, org, actor, requestID string) (domain.Household, error) {
	if zoneID == "" || name == "" || people <= 0 {
		return domain.Household{}, domain.ErrValidation
	}
	item := domain.Household{ID: domain.NewID("house"), ZoneID: zoneID, HeadName: name, People: people, Status: domain.HouseholdPending}
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO households(id,zone_id,head_name,people,status) VALUES(?,?,?,?,?)`, item.ID, item.ZoneID, item.HeadName, item.People, item.Status); err != nil {
			return err
		}
		if s.Audit != nil {
			return s.Audit.Record(ctx, tx, org, actor, "household", item.ID, "register", "success", requestID, item)
		}
		return nil
	})
	return item, err
}
