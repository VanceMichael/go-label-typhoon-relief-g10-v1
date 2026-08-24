package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
	"strings"
)

type Service struct{ DB *sqlite.DB }

func New(db *sqlite.DB) *Service { return &Service{DB: db} }
func (s *Service) Overview(ctx context.Context, id string) (domain.StormOverview, error) {
	var o domain.StormOverview
	var status, opened, closed string
	if err := s.DB.QueryRowContext(ctx, `SELECT id,organization_id,name,status,severity,version,opened_at,COALESCE(closed_at,'') FROM storm_events WHERE id=?`, id).Scan(&o.Storm.ID, &o.Storm.OrganizationID, &o.Storm.Name, &status, &o.Storm.Severity, &o.Storm.Version, &opened, &closed); errors.Is(err, sql.ErrNoRows) {
		return o, domain.ErrNotFound
	} else if err != nil {
		return o, err
	}
	o.Storm.Status = domain.StormStatus(status)
	o.Storm.OpenedAt = sqlite.TimeOrZero(opened)
	o.Storm.ClosedAt = sqlite.TimeOrZero(closed)
	queries := []struct {
		dest *int
		sql  string
	}{{&o.Forecasts, `SELECT COUNT(*) FROM forecast_versions WHERE storm_id=?`}, {&o.Zones, `SELECT COUNT(*) FROM risk_zones WHERE storm_id=?`}, {&o.OpenOrders, `SELECT COUNT(*) FROM evacuation_orders WHERE storm_id=? AND status='published'`}, {&o.OccupiedShelterPlaces, `SELECT COALESCE(SUM(r.people),0) FROM shelter_reservations r JOIN shelters s ON s.id=r.shelter_id JOIN households h ON h.id=r.household_id JOIN risk_zones z ON z.id=h.zone_id WHERE z.storm_id=? AND r.status='active'`}, {&o.OpenTasks, `SELECT COUNT(*) FROM dispatch_tasks WHERE storm_id=? AND status IN ('queued','claimed')`}, {&o.InTransitMovements, `SELECT COUNT(*) FROM supply_movements WHERE storm_id=? AND status='in_transit'`}}
	for _, q := range queries {
		if err := s.DB.QueryRowContext(ctx, q.sql, id).Scan(q.dest); err != nil {
			return o, err
		}
	}
	return o, nil
}

type TaskFilter struct {
	Status, ZoneID string
	Limit, Offset  int
}

func (s *Service) Tasks(ctx context.Context, stormID string, f TaskFilter) (domain.Page[domain.DispatchTask], error) {
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 50
	}
	if f.Offset < 0 {
		f.Offset = 0
	}
	pred := []string{"storm_id=?"}
	args := []any{stormID}
	if f.Status != "" {
		pred = append(pred, "status=?")
		args = append(args, f.Status)
	}
	if f.ZoneID != "" {
		pred = append(pred, "zone_id=?")
		args = append(args, f.ZoneID)
	}
	where := strings.Join(pred, " AND ")
	var total int
	if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM dispatch_tasks WHERE "+where, args...).Scan(&total); err != nil {
		return domain.Page[domain.DispatchTask]{}, err
	}
	query := fmt.Sprintf(`SELECT id,storm_id,COALESCE(zone_id,''),title,status,priority,COALESCE(owner_id,''),COALESCE(lease_until,''),version,COALESCE(report,''),created_at,COALESCE(completed_at,'') FROM dispatch_tasks WHERE %s ORDER BY priority DESC,created_at,id LIMIT ? OFFSET ?`, where)
	args = append(args, f.Limit, f.Offset)
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return domain.Page[domain.DispatchTask]{}, err
	}
	defer rows.Close()
	page := domain.Page[domain.DispatchTask]{Limit: f.Limit, Offset: f.Offset, Total: total}
	for rows.Next() {
		var x domain.DispatchTask
		var status, lease, created, done string
		if err := rows.Scan(&x.ID, &x.StormID, &x.ZoneID, &x.Title, &status, &x.Priority, &x.OwnerID, &lease, &x.Version, &x.Report, &created, &done); err != nil {
			return page, err
		}
		x.Status = domain.TaskStatus(status)
		x.LeaseUntil = sqlite.TimeOrZero(lease)
		x.CreatedAt = sqlite.TimeOrZero(created)
		x.CompletedAt = sqlite.TimeOrZero(done)
		page.Items = append(page.Items, x)
	}
	return page, rows.Err()
}
