package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/alert"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/config"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/dispatch"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/outbox"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/shelter"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
	"log/slog"
	"sync"
	"time"
)

type Runner struct {
	DB     *sqlite.DB
	Config config.Config
	Logger *slog.Logger
	Outbox *outbox.Service
	Alerts *alert.Service
	Sender alert.Sender
	owner  string
	wg     sync.WaitGroup
}

func New(db *sqlite.DB, c config.Config, l *slog.Logger) *Runner {
	o := outbox.New(db)
	return &Runner{DB: db, Config: c, Logger: l, Outbox: o, Alerts: alert.New(db, nil, o), Sender: alert.NewWebhookSender(c.WebhookURL), owner: domain.NewID("worker")}
}
func (r *Runner) Run(ctx context.Context) {
	r.wg.Add(1)
	defer r.wg.Done()
	ticker := time.NewTicker(r.Config.WorkerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Tick(ctx)
		}
	}
}
func (r *Runner) Wait() { r.wg.Wait() }
func (r *Runner) Tick(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	r.deliverOutbox(ctx)
	r.reclaimShelters(ctx)
	r.reclaimDispatch(ctx)
	r.expireSessions(ctx)
}
func (r *Runner) deliverOutbox(ctx context.Context) {
	ev, err := r.Outbox.Claim(ctx, r.owner, r.Config.WorkerLease, time.Now().UTC())
	if errors.Is(err, sql.ErrNoRows) {
		return
	}
	if err != nil {
		r.Logger.Warn("claim outbox", "error", err)
		return
	}
	if ev.EventType == "alert.requested" {
		var p struct{ StormID, OrderID, Channel string }
		if err := json.Unmarshal([]byte(ev.Payload), &p); err != nil {
			_ = r.Outbox.Dead(ctx, ev.ID, r.owner, err)
			return
		}
		if err := r.Sender.Send(ctx, alert.Message{ID: ev.ID, StormID: p.StormID, Channel: p.Channel}); err != nil {
			if ev.Attempts >= 5 {
				_ = r.Outbox.Dead(ctx, ev.ID, r.owner, err)
			} else {
				_ = r.Outbox.Fail(ctx, ev.ID, r.owner, time.Now().Add(backoff(ev.Attempts)), err)
			}
			return
		}
	}
	if err := r.Outbox.Ack(ctx, ev.ID, r.owner); err != nil {
		r.Logger.Warn("ack outbox", "event_id", ev.ID, "error", err)
	}
}
func (r *Runner) reclaimShelters(ctx context.Context) {
	if _, err := shelter.New(r.DB, nil, nil).ReleaseExpired(ctx, time.Now().UTC()); err != nil {
		r.Logger.Warn("release shelters", "error", err)
	}
}
func (r *Runner) reclaimDispatch(ctx context.Context) {
	if _, err := dispatch.New(r.DB, nil, nil, nil).Reclaim(ctx, time.Now().UTC()); err != nil {
		r.Logger.Warn("reclaim dispatch", "error", err)
	}
}
func (r *Runner) expireSessions(ctx context.Context) {
	if _, err := r.DB.ExecContext(ctx, `UPDATE sessions SET revoked_at=expires_at WHERE revoked_at IS NULL AND expires_at<?`, sqlite.FormatTime(time.Now().UTC())); err != nil {
		r.Logger.Warn("expire sessions", "error", err)
	}
}
func backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	if attempts > 6 {
		attempts = 6
	}
	return time.Duration(1<<attempts) * time.Second
}
