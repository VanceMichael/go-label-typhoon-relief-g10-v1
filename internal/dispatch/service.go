package dispatch

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/audit"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/evidence"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/outbox"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
)

type Service struct {
	DB       *sqlite.DB
	Audit    *audit.Service
	Outbox   *outbox.Service
	Evidence *evidence.MemoryStore
	Now      func() time.Time
}

func New(db *sqlite.DB, a *audit.Service, o *outbox.Service, e *evidence.MemoryStore) *Service {
	return &Service{DB: db, Audit: a, Outbox: o, Evidence: e, Now: time.Now}
}
func (s *Service) Create(ctx context.Context, stormID, zoneID, title string, priority int, org, actor, requestID string) (domain.DispatchTask, error) {
	if stormID == "" || title == "" || priority < 1 {
		return domain.DispatchTask{}, domain.ErrValidation
	}
	item := domain.DispatchTask{ID: domain.NewID("task"), StormID: stormID, ZoneID: zoneID, Title: title, Priority: priority, Status: domain.TaskQueued, Version: 1, CreatedAt: s.Now().UTC()}
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO dispatch_tasks(id,storm_id,zone_id,title,status,priority,version,created_at) VALUES(?,?,?,?,?,?,1,?)`, item.ID, item.StormID, sqlite.NullString(item.ZoneID), item.Title, item.Status, item.Priority, sqlite.FormatTime(item.CreatedAt)); err != nil {
			return err
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, org, actor, "dispatch_task", item.ID, "create", "success", requestID, item); err != nil {
				return err
			}
		}
		if s.Outbox != nil {
			return s.Outbox.Enqueue(ctx, tx, "dispatch_task", item.ID, "dispatch.created", item)
		}
		return nil
	})
	return item, err
}
func (s *Service) Claim(ctx context.Context, id, owner, requestID string, lease time.Duration) (domain.DispatchTask, error) {
	if lease <= 0 {
		lease = time.Minute
	}
	var item domain.DispatchTask
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var status, zone, created, until, completed string
		var organizationID string
		var version, priority int
		if err := tx.QueryRowContext(ctx, `SELECT storm_id,COALESCE(zone_id,''),title,status,priority,COALESCE(owner_id,''),COALESCE(lease_until,''),version,COALESCE(report,''),created_at,COALESCE(completed_at,'') FROM dispatch_tasks WHERE id=?`, id).Scan(&item.StormID, &zone, &item.Title, &status, &priority, &item.OwnerID, &until, &version, &item.Report, &created, &completed); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT organization_id FROM storm_events WHERE id=?`, item.StormID).Scan(&organizationID); err != nil {
			return err
		}
		item.ID = id
		item.ZoneID = zone
		item.Status = domain.TaskStatus(status)
		item.Priority = priority
		item.Version = version
		item.CreatedAt = sqlite.TimeOrZero(created)
		item.CompletedAt = sqlite.TimeOrZero(completed)
		if item.Status != domain.TaskQueued {
			return domain.ErrInvalidState
		}
		item.OwnerID = owner
		item.LeaseUntil = s.Now().UTC().Add(lease)
		res, err := tx.ExecContext(ctx, `UPDATE dispatch_tasks SET status='claimed',owner_id=?,lease_until=?,version=version+1 WHERE id=? AND status='queued' AND version=?`, owner, sqlite.FormatTime(item.LeaseUntil), id, version)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return domain.ErrLeaseLost
		}
		item.Status = domain.TaskClaimed
		item.Version++
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, organizationID, owner, "dispatch_task", id, "claim", "success", requestID, map[string]any{"owner": owner}); err != nil {
				return err
			}
		}
		return nil
	})
	return item, err
}
func (s *Service) Report(ctx context.Context, id, owner, proofURI, report, requestID string) error {
	if proofURI == "" || report == "" {
		return domain.ErrValidation
	}
	return s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var status, stored, organizationID string
		var version int
		if err := tx.QueryRowContext(ctx, `SELECT d.status,COALESCE(d.owner_id,''),d.version,s.organization_id FROM dispatch_tasks d JOIN storm_events s ON s.id=d.storm_id WHERE d.id=?`, id).Scan(&status, &stored, &version, &organizationID); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if status != string(domain.TaskClaimed) || stored != owner {
			return domain.ErrLeaseLost
		}
		if s.Evidence != nil {
			reader, err := s.Evidence.Open(ctx, proofURI)
			if err != nil {
				return err
			}
			defer reader.Close()
			if _, err := evidence.Digest(reader); err != nil {
				return err
			}
		}
		res, err := tx.ExecContext(ctx, `UPDATE dispatch_tasks SET status='reported',owner_id=NULL,lease_until=NULL,version=version+1,report=?,completed_at=? WHERE id=? AND status='claimed' AND owner_id=? AND version=?`, report, sqlite.FormatTime(s.Now()), id, owner, version)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return domain.ErrLeaseLost
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, organizationID, owner, "dispatch_task", id, "report", "success", requestID, map[string]string{"proof_uri": proofURI}); err != nil {
				return err
			}
		}
		if s.Outbox != nil {
			return s.Outbox.Enqueue(ctx, tx, "dispatch_task", id, "dispatch.reported", map[string]string{"report": report})
		}
		return nil
	})
}
func (s *Service) Reclaim(ctx context.Context, now time.Time) (int64, error) {
	res, err := s.DB.ExecContext(ctx, `UPDATE dispatch_tasks SET status='queued',owner_id=NULL,lease_until=NULL,version=version+1 WHERE status='claimed' AND lease_until<?`, sqlite.FormatTime(now))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
func (s *Service) Queue(ctx context.Context, stormID string) ([]domain.DispatchTask, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id,storm_id,COALESCE(zone_id,''),title,status,priority,COALESCE(owner_id,''),COALESCE(lease_until,''),version,COALESCE(report,''),created_at,COALESCE(completed_at,'') FROM dispatch_tasks WHERE storm_id=? ORDER BY priority DESC,created_at,id`, stormID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.DispatchTask
	for rows.Next() {
		var item domain.DispatchTask
		var status, lease, created, completed string
		if err := rows.Scan(&item.ID, &item.StormID, &item.ZoneID, &item.Title, &status, &item.Priority, &item.OwnerID, &lease, &item.Version, &item.Report, &created, &completed); err != nil {
			return nil, err
		}
		item.Status = domain.TaskStatus(status)
		item.LeaseUntil = sqlite.TimeOrZero(lease)
		item.CreatedAt = sqlite.TimeOrZero(created)
		item.CompletedAt = sqlite.TimeOrZero(completed)
		result = append(result, item)
	}
	return result, rows.Err()
}
func ValidateReport(report string) error {
	if len(report) < 8 {
		return fmt.Errorf("%w: report too short", domain.ErrValidation)
	}
	return nil
}
