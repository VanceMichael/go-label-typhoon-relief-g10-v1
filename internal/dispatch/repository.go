package dispatch

import (
	"context"
	"database/sql"
	"errors"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
)

type Repository struct{ DB *sqlite.DB }

func NewRepository(db *sqlite.DB) *Repository { return &Repository{DB: db} }
func (r *Repository) ByOwner(ctx context.Context, owner string) ([]domain.DispatchTask, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id,storm_id,COALESCE(zone_id,''),title,status,priority,COALESCE(owner_id,''),COALESCE(lease_until,''),version,COALESCE(report,''),created_at,COALESCE(completed_at,'') FROM dispatch_tasks WHERE owner_id=? ORDER BY priority DESC,created_at`, owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}
func (r *Repository) Queued(ctx context.Context, stormID string, limit int) ([]domain.DispatchTask, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := r.DB.QueryContext(ctx, `SELECT id,storm_id,COALESCE(zone_id,''),title,status,priority,COALESCE(owner_id,''),COALESCE(lease_until,''),version,COALESCE(report,''),created_at,COALESCE(completed_at,'') FROM dispatch_tasks WHERE storm_id=? AND status='queued' ORDER BY priority DESC,created_at,id LIMIT ?`, stormID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}
func scanTasks(rows *sql.Rows) ([]domain.DispatchTask, error) {
	var out []domain.DispatchTask
	for rows.Next() {
		var x domain.DispatchTask
		var st, lease, created, done string
		if err := rows.Scan(&x.ID, &x.StormID, &x.ZoneID, &x.Title, &st, &x.Priority, &x.OwnerID, &lease, &x.Version, &x.Report, &created, &done); err != nil {
			return nil, err
		}
		x.Status = domain.TaskStatus(st)
		x.LeaseUntil = sqlite.TimeOrZero(lease)
		x.CreatedAt = sqlite.TimeOrZero(created)
		x.CompletedAt = sqlite.TimeOrZero(done)
		out = append(out, x)
	}
	return out, rows.Err()
}
func (r *Repository) Find(ctx context.Context, id string) (domain.DispatchTask, error) {
	var x domain.DispatchTask
	var st, lease, created, done string
	err := r.DB.QueryRowContext(ctx, `SELECT id,storm_id,COALESCE(zone_id,''),title,status,priority,COALESCE(owner_id,''),COALESCE(lease_until,''),version,COALESCE(report,''),created_at,COALESCE(completed_at,'') FROM dispatch_tasks WHERE id=?`, id).Scan(&x.ID, &x.StormID, &x.ZoneID, &x.Title, &st, &x.Priority, &x.OwnerID, &lease, &x.Version, &x.Report, &created, &done)
	if errors.Is(err, sql.ErrNoRows) {
		return x, domain.ErrNotFound
	}
	x.Status = domain.TaskStatus(st)
	x.LeaseUntil = sqlite.TimeOrZero(lease)
	x.CreatedAt = sqlite.TimeOrZero(created)
	x.CompletedAt = sqlite.TimeOrZero(done)
	return x, err
}
