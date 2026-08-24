package integration

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/config"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storm"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/worker"
)

func TestEnvironmentConfigurationUsesValidOverridesAndSafeFallbacks(t *testing.T) {
	keys := []string{"HTTP_ADDR", "DATABASE_PATH", "MIGRATIONS_PATH", "WEBHOOK_URL", "SESSION_TTL", "WORKER_INTERVAL", "WORKER_LEASE", "FEATURE_FLAG"}
	old := make(map[string]string)
	for _, key := range keys {
		old[key], _ = os.LookupEnv(key)
		_ = os.Unsetenv(key)
	}
	t.Cleanup(func() {
		for _, key := range keys {
			if value, ok := old[key]; ok {
				_ = os.Setenv(key, value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	})
	defaults := config.Load()
	if defaults.HTTPAddr != ":8080" || defaults.DatabasePath != "./typhoon-relief.db" || defaults.SessionTTL != 8*time.Hour {
		t.Fatalf("defaults=%+v", defaults)
	}
	_ = os.Setenv("HTTP_ADDR", "127.0.0.1:9090")
	_ = os.Setenv("DATABASE_PATH", "/var/lib/typhoon.db")
	_ = os.Setenv("MIGRATIONS_PATH", "/etc/typhoon/migrations")
	_ = os.Setenv("WEBHOOK_URL", "https://alerts.example.test/hook")
	_ = os.Setenv("SESSION_TTL", "45m")
	_ = os.Setenv("WORKER_INTERVAL", "250ms")
	_ = os.Setenv("WORKER_LEASE", "90s")
	_ = os.Setenv("FEATURE_FLAG", "true")
	configured := config.Load()
	if configured.HTTPAddr != "127.0.0.1:9090" || configured.DatabasePath != "/var/lib/typhoon.db" || configured.MigrationsPath != "/etc/typhoon/migrations" {
		t.Fatalf("configured paths=%+v", configured)
	}
	if configured.SessionTTL != 45*time.Minute || configured.WorkerInterval != 250*time.Millisecond || configured.WorkerLease != 90*time.Second || configured.WebhookURL == "" {
		t.Fatalf("configured durations=%+v", configured)
	}
	if !config.Bool("FEATURE_FLAG", false) || !config.Bool("FEATURE_FLAG", true) {
		t.Fatal("boolean configuration parsing failed")
	}
	_ = os.Setenv("SESSION_TTL", "not-a-duration")
	_ = os.Setenv("FEATURE_FLAG", "invalid")
	fallback := config.Load()
	if fallback.SessionTTL != 8*time.Hour || !config.Bool("FEATURE_FLAG", true) {
		t.Fatalf("invalid override fallback=%+v", fallback)
	}
}

func TestWorkerTaskRegistryRejectsInvalidAndDuplicateDefinitions(t *testing.T) {
	scheduler := worker.NewScheduler()
	if err := scheduler.Register("", func(context.Context) error { return nil }); !errors.Is(err, worker.ErrInvalidTask) {
		t.Fatalf("empty task error=%v", err)
	}
	if err := scheduler.Register("nil", nil); !errors.Is(err, worker.ErrInvalidTask) {
		t.Fatalf("nil task error=%v", err)
	}
	if err := scheduler.Register("audit", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("register audit: %v", err)
	}
	if err := scheduler.Register("audit", func(context.Context) error { return nil }); !errors.Is(err, worker.ErrDuplicateTask) {
		t.Fatalf("duplicate task error=%v", err)
	}
	names := scheduler.Names()
	if len(names) != 1 || names[0] != "audit" {
		t.Fatalf("registered names=%v", names)
	}
	if err := scheduler.Run(context.Background(), "audit"); err != nil {
		t.Fatalf("run audit: %v", err)
	}
	if err := scheduler.Run(context.Background(), "missing"); !errors.Is(err, worker.ErrMissingTask) {
		t.Fatalf("missing task error=%v", err)
	}
}

func TestTimeAndDatabaseHelpersPreserveRoundTripPrecision(t *testing.T) {
	h := newHarness(t)
	when := time.Date(2026, 8, 24, 10, 11, 12, 345678901, time.FixedZone("CST", 8*60*60))
	encoded := sqlite.FormatTime(when)
	decoded := sqlite.TimeOrZero(encoded)
	if !decoded.Equal(when.UTC()) {
		t.Fatalf("time round trip encoded=%q decoded=%s original=%s", encoded, decoded, when.UTC())
	}
	if sqlite.NullString("") != nil || sqlite.NullString("value") != "value" {
		t.Fatal("nullable string conversion failed")
	}
	if err := h.db.PingContext(context.Background()); err != nil {
		t.Fatalf("database ping: %v", err)
	}
	if _, err := h.db.ExecContext(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("database query: %v", err)
	}
}

func TestContextDeadlineStopsQueriesBeforeBusinessWork(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	if _, err := h.query.Overview(ctx, "not-created"); err == nil {
		t.Fatal("deadline was not propagated to overview")
	}
	if _, err := h.storm.Create(ctx, domainStormInput(h.org)); err == nil {
		t.Fatal("deadline was not propagated to storm creation")
	}
}

func TestBusinessErrorsRemainDistinctAtServiceBoundary(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.shelter.Reserve(ctx, "missing", "house", 1, time.Hour, h.org, h.user.ID, "missing-shelter"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing shelter error=%v", err)
	}
	if _, err := h.supply.CreateItem(ctx, h.org, "", "Broken", 1, h.user.ID, "bad-item"); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("invalid item error=%v", err)
	}
	if _, err := h.dispatch.Create(ctx, "", "", "", 0, h.org, h.user.ID, "bad-task"); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("invalid task error=%v", err)
	}
	if err := h.evac.Cancel(ctx, "missing", h.org, h.user.ID, "missing-order"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing order error=%v", err)
	}
}

func domainStormInput(org string) storm.CreateInput {
	return storm.CreateInput{OrganizationID: org, Name: "Deadline", Severity: "red", RequestID: "deadline"}
}
