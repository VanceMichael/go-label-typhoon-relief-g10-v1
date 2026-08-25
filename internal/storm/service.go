package storm

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

type CreateInput struct{ OrganizationID, Name, Severity, ActorID, RequestID string }

func (s *Service) Create(ctx context.Context, in CreateInput) (domain.StormEvent, error) {
	if in.OrganizationID == "" || in.Name == "" {
		return domain.StormEvent{}, domain.ErrValidation
	}
	now := s.Now().UTC()
	item := domain.StormEvent{ID: domain.NewID("storm"), OrganizationID: in.OrganizationID, Name: in.Name, Severity: in.Severity, Status: domain.StormMonitoring, Version: 1, OpenedAt: now}
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO storm_events(id,organization_id,name,status,severity,version,opened_at) VALUES(?,?,?,?,?,?,?)`, item.ID, item.OrganizationID, item.Name, item.Status, item.Severity, item.Version, sqlite.FormatTime(now)); err != nil {
			return err
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, in.OrganizationID, in.ActorID, "storm", item.ID, "create", "success", in.RequestID, in); err != nil {
				return err
			}
		}
		if s.Outbox != nil {
			return s.Outbox.Enqueue(ctx, tx, "storm", item.ID, "storm.created", item)
		}
		return nil
	})
	if sqlite.IsConstraint(err) {
		return domain.StormEvent{}, domain.ErrConflict
	}
	return item, err
}
func (s *Service) Get(ctx context.Context, id string) (domain.StormEvent, error) {
	var item domain.StormEvent
	var status, opened, closed string
	err := s.DB.QueryRowContext(ctx, `SELECT id,organization_id,name,status,severity,version,opened_at,COALESCE(closed_at,'') FROM storm_events WHERE id=?`, id).Scan(&item.ID, &item.OrganizationID, &item.Name, &status, &item.Severity, &item.Version, &opened, &closed)
	if errors.Is(err, sql.ErrNoRows) {
		return item, domain.ErrNotFound
	}
	item.Status = domain.StormStatus(status)
	item.OpenedAt = sqlite.TimeOrZero(opened)
	item.ClosedAt = sqlite.TimeOrZero(closed)
	return item, err
}

type ForecastInput struct {
	StormID, Source, WarningLevel      string
	WindKPH                            float64
	ValidUntil                         time.Time
	ActorID, OrganizationID, RequestID string
}

func (s *Service) PublishForecast(ctx context.Context, in ForecastInput) (domain.Forecast, error) {
	if in.StormID == "" || in.Source == "" || in.WarningLevel == "" || in.ValidUntil.IsZero() {
		return domain.Forecast{}, domain.ErrValidation
	}
	now := s.Now().UTC()
	item := domain.Forecast{ID: domain.NewID("forecast"), StormID: in.StormID, Source: in.Source, WarningLevel: in.WarningLevel, WindKPH: in.WindKPH, ValidUntil: in.ValidUntil, PublishedAt: now}
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM forecast_versions WHERE storm_id=?`, in.StormID).Scan(&item.Version); err != nil {
			return err
		}
		item.Version++
		if _, err := tx.ExecContext(ctx, `INSERT INTO forecast_versions(id,storm_id,version,source,wind_kph,warning_level,valid_until,published_at) VALUES(?,?,?,?,?,?,?,?)`, item.ID, item.StormID, item.Version, item.Source, item.WindKPH, item.WarningLevel, sqlite.FormatTime(item.ValidUntil), sqlite.FormatTime(now)); err != nil {
			return err
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, in.OrganizationID, in.ActorID, "forecast", item.ID, "publish", "success", in.RequestID, item); err != nil {
				return err
			}
		}
		if s.Outbox != nil {
			return s.Outbox.Enqueue(ctx, tx, "storm", in.StormID, "forecast.published", item)
		}
		return nil
	})
	if sqlite.IsConstraint(err) {
		return domain.Forecast{}, domain.ErrConflict
	}
	return item, err
}
func (s *Service) AddZone(ctx context.Context, stormID, name, risk, org, actor, requestID string) (domain.RiskZone, error) {
	if stormID == "" || name == "" || risk == "" {
		return domain.RiskZone{}, domain.ErrValidation
	}
	item := domain.RiskZone{ID: domain.NewID("zone"), StormID: stormID, Name: name, RiskLevel: risk}
	err := s.DB.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO risk_zones(id,storm_id,name,risk_level,confirmed) VALUES(?,?,?,?,0)`, item.ID, item.StormID, item.Name, item.RiskLevel); err != nil {
			return err
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, org, actor, "risk_zone", item.ID, "create", "success", requestID, item); err != nil {
				return err
			}
		}
		return nil
	})
	if sqlite.IsConstraint(err) {
		return domain.RiskZone{}, domain.ErrConflict
	}
	return item, err
}
func (s *Service) ConfirmZone(ctx context.Context, id, org, actor, requestID string) error {
	return s.DB.Tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE risk_zones SET confirmed=1 WHERE id=? AND confirmed=0`, id)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return domain.ErrConflict
		}
		if s.Audit != nil {
			return s.Audit.Record(ctx, tx, org, actor, "risk_zone", id, "confirm", "success", requestID, map[string]bool{"confirmed": true})
		}
		return nil
	})
}
func (s *Service) Activate(ctx context.Context, id, actor, requestID string) error {
	return s.move(ctx, id, actor, requestID, domain.StormActive)
}
func (s *Service) move(ctx context.Context, id, actor, requestID string, next domain.StormStatus) error {
	return s.DB.Tx(ctx, func(tx *sql.Tx) error {
		var status, org string
		var version int
		if err := tx.QueryRowContext(ctx, `SELECT status,organization_id,version FROM storm_events WHERE id=?`, id).Scan(&status, &org, &version); errors.Is(err, sql.ErrNoRows) {
			return domain.ErrNotFound
		} else if err != nil {
			return err
		}
		if !domain.StormStatus(status).CanMoveTo(next) {
			return domain.ErrInvalidState
		}
		res, err := tx.ExecContext(ctx, `UPDATE storm_events SET status=?,version=version+1 WHERE id=? AND version=?`, next, id, version)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return domain.ErrConflict
		}
		if s.Audit != nil {
			if err := s.Audit.Record(ctx, tx, org, actor, "storm", id, "status.move", "success", requestID, map[string]string{"to": string(next)}); err != nil {
				return err
			}
		}
		if s.Outbox != nil {
			return s.Outbox.Enqueue(ctx, tx, "storm", id, "storm.status_changed", map[string]string{"to": string(next)})
		}
		return nil
	})
}
func (s *Service) ForecastHistory(ctx context.Context, id string) ([]domain.Forecast, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id,storm_id,version,source,wind_kph,warning_level,valid_until,published_at FROM forecast_versions WHERE storm_id=? ORDER BY version`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.Forecast
	for rows.Next() {
		var f domain.Forecast
		var valid, pub string
		if err := rows.Scan(&f.ID, &f.StormID, &f.Version, &f.Source, &f.WindKPH, &f.WarningLevel, &valid, &pub); err != nil {
			return nil, err
		}
		f.ValidUntil = sqlite.TimeOrZero(valid)
		f.PublishedAt = sqlite.TimeOrZero(pub)
		result = append(result, f)
	}
	return result, rows.Err()
}
func ValidateForecast(f domain.Forecast) error {
	if f.WindKPH < 0 {
		return fmt.Errorf("%w: wind", domain.ErrValidation)
	}
	if f.ValidUntil.Before(f.PublishedAt) {
		return fmt.Errorf("%w: forecast interval", domain.ErrValidation)
	}
	return nil
}
