package audit

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
)

type Service struct {
	DB  *sqlite.DB
	Now func() time.Time
}

func New(db *sqlite.DB) *Service { return &Service{DB: db, Now: time.Now} }
func (s *Service) Record(ctx context.Context, tx *sql.Tx, org, actor, objectType, objectID, action, result, requestID string, detail any) error {
	body, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("audit detail: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO audit_events(id,organization_id,actor_id,object_type,object_id,action,result,request_id,detail,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, domain.NewID("aud"), org, sqlite.NullString(actor), objectType, objectID, action, result, requestID, string(body), sqlite.FormatTime(s.Now().UTC()))
	return err
}
func (s *Service) List(ctx context.Context, objectType, objectID string, limit, offset int) (domain.Page[domain.AuditEvent], error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	var total int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_events WHERE object_type=? AND object_id=?`, objectType, objectID).Scan(&total); err != nil {
		return domain.Page[domain.AuditEvent]{}, err
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT id,organization_id,COALESCE(actor_id,''),object_type,object_id,action,result,request_id,detail,created_at FROM audit_events WHERE object_type=? AND object_id=? ORDER BY created_at,id LIMIT ? OFFSET ?`, objectType, objectID, limit, offset)
	if err != nil {
		return domain.Page[domain.AuditEvent]{}, err
	}
	defer rows.Close()
	page := domain.Page[domain.AuditEvent]{Limit: limit, Offset: offset, Total: total}
	for rows.Next() {
		var e domain.AuditEvent
		var created string
		if err := rows.Scan(&e.ID, &e.OrganizationID, &e.ActorID, &e.ObjectType, &e.ObjectID, &e.Action, &e.Result, &e.RequestID, &e.Detail, &created); err != nil {
			return page, err
		}
		e.CreatedAt = sqlite.TimeOrZero(created)
		page.Items = append(page.Items, e)
	}
	return page, sqlite.RequireRows(rows)
}

type Exporter struct{ DB *sqlite.DB }

func NewExporter(db *sqlite.DB) *Exporter { return &Exporter{DB: db} }
func (e *Exporter) JSONL(ctx context.Context, objectType, objectID string, dst io.Writer) error {
	rows, err := e.DB.QueryContext(ctx, `SELECT id,organization_id,COALESCE(actor_id,''),object_type,object_id,action,result,request_id,detail,created_at FROM audit_events WHERE object_type=? AND object_id=? ORDER BY created_at,id`, objectType, objectID)
	if err != nil {
		return err
	}
	defer rows.Close()
	seq := 0
	for rows.Next() {
		var ev domain.AuditEvent
		var created string
		if err := rows.Scan(&ev.ID, &ev.OrganizationID, &ev.ActorID, &ev.ObjectType, &ev.ObjectID, &ev.Action, &ev.Result, &ev.RequestID, &ev.Detail, &created); err != nil {
			return err
		}
		ev.CreatedAt = sqlite.TimeOrZero(created)
		seq++
		body, err := json.Marshal(map[string]any{"event": ev, "sequence": seq})
		if err != nil {
			return err
		}
		if _, err := dst.Write(append(body, '\n')); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("audit export rows: %w", err)
	}
	return nil
}
