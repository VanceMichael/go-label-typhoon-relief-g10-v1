package supply

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
func (s *Service) CreateItem(ctx context.Context, org, sku, name string, quantity int, actor, requestID string) (domain.SupplyItem, error) {
	if org == "" || sku == "" || name == "" || quantity < 0 {
		return domain.SupplyItem{}, domain.ErrValidation
	}
	item := domain.SupplyItem{ID: domain.NewID("item"), OrganizationID: org, SKU: sku, Name: name, Quantity: quantity, Version: 1}
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO supply_items(id,organization_id,sku,name,quantity,version) VALUES(?,?,?,?,?,1)`, item.ID, org, sku, name, quantity); err != nil {
			return err
		}
		if s.Audit != nil {
			return s.Audit.Record(ctx, tx, org, actor, "supply_item", item.ID, "create", "success", requestID, item)
		}
		return nil
	})
	if sqlite.IsConstraint(err) {
		return domain.SupplyItem{}, domain.ErrConflict
	}
	return item, err
}
func (s *Service) Dispatch(ctx context.Context, itemID, stormID, shelterID, org, actor, requestID string, quantity int) (domain.SupplyMovement, error) {
	if itemID == "" || stormID == "" || shelterID == "" || quantity <= 0 {
		return domain.SupplyMovement{}, domain.ErrValidation
	}
	item := domain.SupplyMovement{ID: domain.NewID("move"), ItemID: itemID, StormID: stormID, FromOrgID: org, ShelterID: shelterID, Quantity: quantity, Status: domain.MovementInTransit, CreatedAt: s.Now().UTC()}
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var available, reserved, version int
		if err := tx.QueryRowContext(ctx, `SELECT quantity,reserved,version FROM supply_items WHERE id=? AND organization_id=?`, itemID, org).Scan(&available, &reserved, &version); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if available-reserved < quantity {
			return fmt.Errorf("%w: inventory", domain.ErrConflict)
		}
		var shelterStatus string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM shelters WHERE id=?`, shelterID).Scan(&shelterStatus); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if shelterStatus != string(domain.ShelterOpen) {
			return domain.ErrInvalidState
		}
		res, err := tx.ExecContext(ctx, `UPDATE supply_items SET reserved=reserved+?,version=version+1 WHERE id=? AND version=? AND quantity-reserved>=?`, quantity, itemID, version, quantity)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return domain.ErrConflict
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO supply_movements(id,item_id,storm_id,from_org_id,shelter_id,quantity,status,created_at) VALUES(?,?,?,?,?,?,?,?)`, item.ID, item.ItemID, item.StormID, item.FromOrgID, item.ShelterID, item.Quantity, item.Status, sqlite.FormatTime(item.CreatedAt)); err != nil {
			return err
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, org, actor, "supply_movement", item.ID, "dispatch", "success", requestID, item); err != nil {
				return err
			}
		}
		if s.Outbox != nil {
			return s.Outbox.Enqueue(ctx, tx, "supply_movement", item.ID, "supply.dispatched", item)
		}
		return nil
	})
	return item, err
}
func (s *Service) Deliver(ctx context.Context, id, org, actor, requestID string) error {
	return s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var item, shelter, status string
		var qty int
		if err := tx.QueryRowContext(ctx, `SELECT item_id,shelter_id,status,quantity FROM supply_movements WHERE id=?`, id).Scan(&item, &shelter, &status, &qty); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if status != string(domain.MovementInTransit) {
			return domain.ErrInvalidState
		}
		res, err := tx.ExecContext(ctx, `UPDATE supply_movements SET status='delivered',delivered_at=? WHERE id=? AND status='in_transit'`, sqlite.FormatTime(s.Now()), id)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return domain.ErrConflict
		}
		res, err = tx.ExecContext(ctx, `UPDATE supply_items SET reserved=reserved-?,quantity=quantity-? WHERE id=? AND reserved>=? AND quantity>=?`, qty, qty, item, qty, qty)
		if err != nil {
			return err
		}
		updated, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if updated != 1 {
			return domain.ErrConflict
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, org, actor, "supply_movement", id, "deliver", "success", requestID, map[string]any{"shelter": shelter, "quantity": qty}); err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *Service) Cancel(ctx context.Context, id, org, actor, requestID string) error {
	return s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var item, status string
		var qty int
		if err := tx.QueryRowContext(ctx, `SELECT item_id,status,quantity FROM supply_movements WHERE id=?`, id).Scan(&item, &status, &qty); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if status != string(domain.MovementInTransit) {
			return domain.ErrInvalidState
		}
		if _, err := tx.ExecContext(ctx, `UPDATE supply_movements SET status='cancelled' WHERE id=? AND status='in_transit'`, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE supply_items SET reserved=reserved-? WHERE id=? AND reserved>=?`, qty, item, qty); err != nil {
			return err
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, org, actor, "supply_movement", id, "cancel", "success", requestID, map[string]int{"quantity": qty}); err != nil {
				if sqlite.IsConstraint(err) {
					return nil
				}
				return nil
			}
		}
		return nil
	})
}
func (s *Service) Inventory(ctx context.Context, org string) ([]domain.SupplyItem, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id,organization_id,sku,name,quantity,reserved,version FROM supply_items WHERE organization_id=? ORDER BY sku`, org)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.SupplyItem
	for rows.Next() {
		var x domain.SupplyItem
		if err := rows.Scan(&x.ID, &x.OrganizationID, &x.SKU, &x.Name, &x.Quantity, &x.Reserved, &x.Version); err != nil {
			return nil, err
		}
		result = append(result, x)
	}
	return result, rows.Err()
}
