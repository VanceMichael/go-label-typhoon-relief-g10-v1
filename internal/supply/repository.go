package supply

import (
	"context"
	"database/sql"
	"errors"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
)

type Repository struct{ DB *sqlite.DB }

func NewRepository(db *sqlite.DB) *Repository { return &Repository{DB: db} }
func (r *Repository) Movements(ctx context.Context, stormID string) ([]domain.SupplyMovement, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id,item_id,storm_id,from_org_id,shelter_id,quantity,status,created_at,COALESCE(delivered_at,'') FROM supply_movements WHERE storm_id=? ORDER BY created_at,id`, stormID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.SupplyMovement
	for rows.Next() {
		var x domain.SupplyMovement
		var st, created, del string
		if err := rows.Scan(&x.ID, &x.ItemID, &x.StormID, &x.FromOrgID, &x.ShelterID, &x.Quantity, &st, &created, &del); err != nil {
			return nil, err
		}
		x.Status = domain.MovementStatus(st)
		x.CreatedAt = sqlite.TimeOrZero(created)
		x.DeliveredAt = sqlite.TimeOrZero(del)
		out = append(out, x)
	}
	return out, rows.Err()
}
func (r *Repository) FindItem(ctx context.Context, id string) (domain.SupplyItem, error) {
	var x domain.SupplyItem
	err := r.DB.QueryRowContext(ctx, `SELECT id,organization_id,sku,name,quantity,reserved,version FROM supply_items WHERE id=?`, id).Scan(&x.ID, &x.OrganizationID, &x.SKU, &x.Name, &x.Quantity, &x.Reserved, &x.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return x, domain.ErrNotFound
	}
	return x, err
}
func (r *Repository) InTransit(ctx context.Context, stormID string) (int, error) {
	var n int
	err := r.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM supply_movements WHERE storm_id=? AND status='in_transit'`, stormID).Scan(&n)
	return n, err
}
func (r *Repository) DeliveredQuantity(ctx context.Context, stormID, shelterID string) (int, error) {
	var n int
	err := r.DB.QueryRowContext(ctx, `SELECT COALESCE(SUM(quantity),0) FROM supply_movements WHERE storm_id=? AND shelter_id=? AND status='delivered'`, stormID, shelterID).Scan(&n)
	return n, err
}
