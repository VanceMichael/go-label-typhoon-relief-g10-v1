package storm

import (
	"context"
	"database/sql"
	"errors"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
)

type Repository struct{ DB *sqlite.DB }

func NewRepository(db *sqlite.DB) *Repository { return &Repository{DB: db} }
func (r *Repository) List(ctx context.Context, org string, limit, offset int) (domain.Page[domain.StormEvent], error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	var total int
	if err := r.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM storm_events WHERE organization_id=?`, org).Scan(&total); err != nil {
		return domain.Page[domain.StormEvent]{}, err
	}
	rows, err := r.DB.QueryContext(ctx, `SELECT id,organization_id,name,status,severity,version,opened_at,COALESCE(closed_at,'') FROM storm_events WHERE organization_id=? ORDER BY opened_at DESC,id DESC LIMIT ? OFFSET ?`, org, limit, offset)
	if err != nil {
		return domain.Page[domain.StormEvent]{}, err
	}
	defer rows.Close()
	page := domain.Page[domain.StormEvent]{Limit: limit, Offset: offset, Total: total}
	for rows.Next() {
		var x domain.StormEvent
		var st, opened, closed string
		if err := rows.Scan(&x.ID, &x.OrganizationID, &x.Name, &st, &x.Severity, &x.Version, &opened, &closed); err != nil {
			return page, err
		}
		x.Status = domain.StormStatus(st)
		x.OpenedAt = sqlite.TimeOrZero(opened)
		x.ClosedAt = sqlite.TimeOrZero(closed)
		page.Items = append(page.Items, x)
	}
	return page, rows.Err()
}
func (r *Repository) Zones(ctx context.Context, stormID string) ([]domain.RiskZone, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id,storm_id,name,risk_level,confirmed FROM risk_zones WHERE storm_id=? ORDER BY name,id`, stormID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.RiskZone
	for rows.Next() {
		var z domain.RiskZone
		var confirmed int
		if err := rows.Scan(&z.ID, &z.StormID, &z.Name, &z.RiskLevel, &confirmed); err != nil {
			return nil, err
		}
		z.Confirmed = confirmed != 0
		out = append(out, z)
	}
	return out, rows.Err()
}
func (r *Repository) LatestForecast(ctx context.Context, stormID string) (domain.Forecast, error) {
	var f domain.Forecast
	var valid, pub string
	err := r.DB.QueryRowContext(ctx, `SELECT id,storm_id,version,source,wind_kph,warning_level,valid_until,published_at FROM forecast_versions WHERE storm_id=? ORDER BY version DESC LIMIT 1`, stormID).Scan(&f.ID, &f.StormID, &f.Version, &f.Source, &f.WindKPH, &f.WarningLevel, &valid, &pub)
	if errors.Is(err, sql.ErrNoRows) {
		return f, domain.ErrNotFound
	}
	f.ValidUntil = sqlite.TimeOrZero(valid)
	f.PublishedAt = sqlite.TimeOrZero(pub)
	return f, err
}
func (r *Repository) EnsureOpen(ctx context.Context, id string) error {
	var status string
	err := r.DB.QueryRowContext(ctx, `SELECT status FROM storm_events WHERE id=?`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ErrNotFound
	}
	if err != nil {
		return err
	}
	if status == string(domain.StormClosed) {
		return domain.ErrInvalidState
	}
	return nil
}
