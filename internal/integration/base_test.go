package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/alert"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/audit"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/auth"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/config"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/dispatch"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/evacuation"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/evidence"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/httpapi"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/outbox"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/query"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/shelter"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storm"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/supply"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/worker"
)

type harness struct {
	t        *testing.T
	db       *sqlite.DB
	org      string
	user     domain.User
	audit    *audit.Service
	outbox   *outbox.Service
	storm    *storm.Service
	shelter  *shelter.Service
	evac     *evacuation.Service
	dispatch *dispatch.Service
	supply   *supply.Service
	alert    *alert.Service
	query    *query.Service
	evidence *evidence.MemoryStore
	clock    time.Time
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	dbPath := filepath.Join(t.TempDir(), "typhoon.db")
	db, err := sqlite.Open(dbPath, filepath.Join(root, "migrations"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := auth.Bootstrap(context.Background(), db, "commander@example.test", "secret"); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	var org string
	var userID string
	if err := db.QueryRow(`SELECT organization_id,id FROM users WHERE email=?`, "commander@example.test").Scan(&org, &userID); err != nil {
		t.Fatalf("load bootstrap user: %v", err)
	}
	var role string
	if err := db.QueryRow(`SELECT role FROM users WHERE id=?`, userID).Scan(&role); err != nil {
		t.Fatalf("load role: %v", err)
	}
	base := time.Date(2026, 8, 24, 9, 0, 0, 0, time.UTC)
	a := audit.New(db)
	o := outbox.New(db)
	e := evidence.NewMemoryStore()
	h := &harness{t: t, db: db, org: org, user: domain.User{ID: userID, OrganizationID: org, Email: "commander@example.test", Role: role, Active: true}, audit: a, outbox: o, evidence: e, clock: base}
	h.audit.Now = func() time.Time { return h.clock }
	h.outbox.Now = func() time.Time { return h.clock }
	h.storm = storm.New(db, a, o)
	h.storm.Now = func() time.Time { return h.clock }
	h.shelter = shelter.New(db, a, o)
	h.shelter.Now = func() time.Time { return h.clock }
	h.evac = evacuation.New(db, a, o)
	h.evac.Now = func() time.Time { return h.clock }
	h.dispatch = dispatch.New(db, a, o, e)
	h.dispatch.Now = func() time.Time { return h.clock }
	h.supply = supply.New(db, a, o)
	h.supply.Now = func() time.Time { return h.clock }
	h.alert = alert.New(db, a, o)
	h.alert.Now = func() time.Time { return h.clock }
	h.query = query.New(db)
	return h
}

func (h *harness) activeStorm() (domain.StormEvent, domain.RiskZone) {
	h.t.Helper()
	ctx := context.Background()
	s, err := h.storm.Create(ctx, storm.CreateInput{OrganizationID: h.org, Name: "Typhoon Lantern", Severity: "red", ActorID: h.user.ID, RequestID: "req-storm"})
	if err != nil {
		h.t.Fatalf("create storm: %v", err)
	}
	if _, err := h.storm.PublishForecast(ctx, storm.ForecastInput{StormID: s.ID, Source: "national-observatory", WarningLevel: "red", WindKPH: 145, ValidUntil: h.clock.Add(6 * time.Hour), OrganizationID: h.org, ActorID: h.user.ID, RequestID: "req-forecast"}); err != nil {
		h.t.Fatalf("publish forecast: %v", err)
	}
	z, err := h.storm.AddZone(ctx, s.ID, "East Coast", "high", h.org, h.user.ID, "req-zone")
	if err != nil {
		h.t.Fatalf("add zone: %v", err)
	}
	if err := h.storm.ConfirmZone(ctx, z.ID, h.org, h.user.ID, "req-zone-confirm"); err != nil {
		h.t.Fatalf("confirm zone: %v", err)
	}
	if err := h.storm.Activate(ctx, s.ID, h.user.ID, "req-activate"); err != nil {
		h.t.Fatalf("activate storm: %v", err)
	}
	s, err = h.storm.Get(ctx, s.ID)
	if err != nil {
		h.t.Fatalf("reload active storm: %v", err)
	}
	return s, z
}

func (h *harness) openShelter(capacity int) domain.Shelter {
	h.t.Helper()
	item, err := h.shelter.Create(context.Background(), shelter.CreateInput{OrganizationID: h.org, Name: fmt.Sprintf("Shelter %d", capacity), Address: "Harbor Road", Capacity: capacity, ActorID: h.user.ID, RequestID: "req-shelter"})
	if err != nil {
		h.t.Fatalf("create shelter: %v", err)
	}
	return item
}

func (h *harness) household(zone domain.RiskZone, people int, name string) domain.Household {
	h.t.Helper()
	item, err := h.evac.AddHousehold(context.Background(), zone.ID, name, people, h.org, h.user.ID, "req-household")
	if err != nil {
		h.t.Fatalf("add household: %v", err)
	}
	return item
}

func (h *harness) publishedOrder(s domain.StormEvent, z domain.RiskZone, requestID string) domain.EvacuationOrder {
	h.t.Helper()
	item, err := h.evac.Create(context.Background(), s.ID, z.ID, h.org, h.user.ID, requestID+"-create")
	if err != nil {
		h.t.Fatalf("create published order: %v", err)
	}
	if err := h.evac.Publish(context.Background(), item.ID, h.org, h.user.ID, requestID+"-publish"); err != nil {
		h.t.Fatalf("publish order: %v", err)
	}
	return item
}

func TestMigrationCreatesRelationsAndRestartRetainsFacts(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	dbPath := filepath.Join(t.TempDir(), "restart.db")
	db, err := sqlite.Open(dbPath, filepath.Join(root, "migrations"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := auth.Bootstrap(context.Background(), db, "restart@example.test", "secret"); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	var before int
	if err := db.QueryRow(`SELECT COUNT(*) FROM organizations`).Scan(&before); err != nil {
		t.Fatalf("count organizations: %v", err)
	}
	if before != 1 {
		t.Fatalf("organizations=%d, want one", before)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db, err = sqlite.Open(dbPath, filepath.Join(root, "migrations"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if err := db.Ready(context.Background()); err != nil {
		t.Fatalf("ready after restart: %v", err)
	}
	var version int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE version=1`).Scan(&version); err != nil {
		t.Fatalf("migration record: %v", err)
	}
	if version != 1 {
		t.Fatalf("migration records=%d, want one", version)
	}
	if err := auth.Bootstrap(context.Background(), db, "restart@example.test", "secret"); err != nil {
		t.Fatalf("idempotent bootstrap: %v", err)
	}
	var users int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE email=?`, "restart@example.test").Scan(&users); err != nil {
		t.Fatalf("users after bootstrap: %v", err)
	}
	if users != 1 {
		t.Fatalf("users=%d, want one after repeated bootstrap", users)
	}
}

func TestTransactionRollbackLeavesNoStormFacts(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, err := h.storm.Create(ctx, storm.CreateInput{OrganizationID: "missing-org", Name: "Rejected", Severity: "red", ActorID: h.user.ID, RequestID: "rollback"})
	if err == nil {
		t.Fatal("expected foreign key failure")
	}
	var count int
	if err := h.db.QueryRow(`SELECT COUNT(*) FROM storm_events WHERE name=?`, "Rejected").Scan(&count); err != nil {
		t.Fatalf("query storm: %v", err)
	}
	if count != 0 {
		t.Fatalf("rolled-back storm count=%d", count)
	}
	var audits int
	if err := h.db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE request_id=?`, "rollback").Scan(&audits); err != nil {
		t.Fatalf("query audit: %v", err)
	}
	if audits != 0 {
		t.Fatalf("rolled-back audits=%d", audits)
	}
}

func TestAuthenticationSessionLifecycleAndRoleRules(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	a := auth.New(h.db, time.Hour)
	result, err := a.Login(ctx, "commander@example.test", "secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if result.Token == "" || result.User.Role != string(domain.RoleCommander) {
		t.Fatalf("unexpected login result: %+v", result)
	}
	u, err := a.Authenticate(ctx, result.Token)
	if err != nil || u.ID != h.user.ID {
		t.Fatalf("authenticate: user=%+v err=%v", u, err)
	}
	if err := a.Logout(ctx, u, result.Token, "logout"); err != nil {
		t.Fatalf("logout: %v", err)
	}
	if _, err := a.Authenticate(ctx, result.Token); !errors.Is(err, domain.ErrUnauthorized) {
		t.Fatalf("revoked token error=%v", err)
	}
	if _, err := a.Login(ctx, "commander@example.test", "wrong"); !errors.Is(err, domain.ErrUnauthorized) {
		t.Fatalf("wrong password error=%v", err)
	}
	if err := auth.RequireRole(domain.User{Role: string(domain.RoleShelter)}, "close_storm"); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("operator close role error=%v", err)
	}
	if err := auth.RequireRole(domain.User{Role: string(domain.RoleCommander)}, "close_storm"); err != nil {
		t.Fatalf("commander close role: %v", err)
	}
}

func TestAuthenticationExpirationIsRejected(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	result, err := auth.New(h.db, time.Nanosecond).Login(ctx, "commander@example.test", "secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if _, err := auth.New(h.db, time.Hour).Authenticate(ctx, result.Token); !errors.Is(err, domain.ErrUnauthorized) {
		t.Fatalf("expired token error=%v", err)
	}
	if err := auth.New(h.db, time.Hour).Expire(ctx); err != nil {
		t.Fatalf("expire sessions: %v", err)
	}
	var revoked string
	if err := h.db.QueryRow(`SELECT COALESCE(revoked_at,'') FROM sessions WHERE token_hash IS NOT NULL ORDER BY created_at DESC LIMIT 1`).Scan(&revoked); err != nil {
		t.Fatalf("revocation marker: %v", err)
	}
	if revoked == "" {
		t.Fatal("expire did not persist revoked_at")
	}
}

func TestStormStateMachineForecastHistoryAndZoneQueries(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	s, z := h.activeStorm()
	if s.Status != domain.StormActive {
		t.Fatalf("status=%s, want active", s.Status)
	}
	if err := h.storm.Activate(ctx, s.ID, h.user.ID, "duplicate"); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("duplicate activate error=%v", err)
	}
	history, err := h.storm.ForecastHistory(ctx, s.ID)
	if err != nil {
		t.Fatalf("forecast history: %v", err)
	}
	if len(history) != 1 || history[0].Version != 1 {
		t.Fatalf("forecast history=%+v", history)
	}
	zones, err := storm.NewRepository(h.db).Zones(ctx, s.ID)
	if err != nil {
		t.Fatalf("zones: %v", err)
	}
	if len(zones) != 1 || zones[0].ID != z.ID || !zones[0].Confirmed {
		t.Fatalf("zones=%+v", zones)
	}
	if _, err := h.storm.AddZone(ctx, s.ID, z.Name, "high", h.org, h.user.ID, "duplicate-zone"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("duplicate zone error=%v", err)
	}
	page, err := storm.NewRepository(h.db).List(ctx, h.org, 1, 0)
	if err != nil {
		t.Fatalf("storm page: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].ID != s.ID {
		t.Fatalf("storm page=%+v", page)
	}
}

func TestForecastValidationAndVersionedAppend(t *testing.T) {
	h := newHarness(t)
	s, _ := h.activeStorm()
	ctx := context.Background()
	if _, err := h.storm.PublishForecast(ctx, storm.ForecastInput{StormID: s.ID, Source: "", WarningLevel: "red", ValidUntil: h.clock.Add(time.Hour)}); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("empty source error=%v", err)
	}
	second, err := h.storm.PublishForecast(ctx, storm.ForecastInput{StormID: s.ID, Source: "regional-center", WarningLevel: "orange", WindKPH: 100, ValidUntil: h.clock.Add(12 * time.Hour), OrganizationID: h.org, ActorID: h.user.ID, RequestID: "forecast-two"})
	if err != nil {
		t.Fatalf("second forecast: %v", err)
	}
	if second.Version != 2 {
		t.Fatalf("version=%d, want two", second.Version)
	}
	latest, err := storm.NewRepository(h.db).LatestForecast(ctx, s.ID)
	if err != nil || latest.ID != second.ID || latest.Version != 2 {
		t.Fatalf("latest=%+v err=%v", latest, err)
	}
	if err := storm.ValidateForecast(domain.Forecast{WindKPH: 10, PublishedAt: h.clock, ValidUntil: h.clock.Add(-time.Minute)}); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("invalid forecast error=%v", err)
	}
}

func TestShelterReservationCapacityReleaseAndRepository(t *testing.T) {
	h := newHarness(t)
	shelterItem := h.openShelter(4)
	ctx := context.Background()
	first, err := h.shelter.Reserve(ctx, shelterItem.ID, "house-1", 3, time.Hour, h.org, h.user.ID, "reserve-one")
	if err != nil {
		t.Fatalf("reserve first: %v", err)
	}
	if first.People != 3 || first.Status != "active" {
		t.Fatalf("reservation=%+v", first)
	}
	if _, err := h.shelter.Reserve(ctx, shelterItem.ID, "house-1", 1, time.Hour, h.org, h.user.ID, "duplicate-house"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("duplicate reservation error=%v", err)
	}
	if _, err := h.shelter.Reserve(ctx, shelterItem.ID, "house-2", 2, time.Hour, h.org, h.user.ID, "too-large"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("capacity error=%v", err)
	}
	items, err := shelter.NewRepository(h.db).Reservations(ctx, shelterItem.ID)
	if err != nil || len(items) != 1 || items[0].ID != first.ID {
		t.Fatalf("reservations=%+v err=%v", items, err)
	}
	capacity, reserved, err := shelter.NewRepository(h.db).Capacity(ctx, shelterItem.ID)
	if err != nil || capacity != 4 || reserved != 3 {
		t.Fatalf("capacity=%d reserved=%d err=%v", capacity, reserved, err)
	}
	released, err := h.shelter.ReleaseExpired(ctx, h.clock.Add(2*time.Hour))
	if err != nil || released != 1 {
		t.Fatalf("release count=%d err=%v", released, err)
	}
	capacity, reserved, err = shelter.NewRepository(h.db).Capacity(ctx, shelterItem.ID)
	if err != nil || reserved != 0 || capacity != 4 {
		t.Fatalf("released capacity=%d reserved=%d err=%v", capacity, reserved, err)
	}
	items, err = shelter.NewRepository(h.db).Reservations(ctx, shelterItem.ID)
	if err != nil || len(items) != 1 || items[0].Status != "expired" {
		t.Fatalf("expired reservations=%+v err=%v", items, err)
	}
}

func TestConcurrentShelterReservationHasOneWinner(t *testing.T) {
	h := newHarness(t)
	item := h.openShelter(1)
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			_, err := h.shelter.Reserve(context.Background(), item.ID, fmt.Sprintf("house-%d", n), 1, time.Hour, h.org, h.user.ID, fmt.Sprintf("parallel-%d", n))
			results <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		} else if !errors.Is(err, domain.ErrConflict) && !strings.Contains(strings.ToLower(err.Error()), "locked") {
			t.Fatalf("unexpected concurrent error: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("successful reservations=%d, want exactly one", wins)
	}
	_, reserved, err := shelter.NewRepository(h.db).Capacity(context.Background(), item.ID)
	if err != nil {
		t.Fatalf("capacity after race: %v", err)
	}
	if reserved != 1 {
		t.Fatalf("reserved=%d, want one", reserved)
	}
}

func TestEvacuationPublishConfirmAndCancelRules(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	ctx := context.Background()
	order, err := h.evac.Create(ctx, s.ID, z.ID, h.org, h.user.ID, "order-create")
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	if err := h.evac.Publish(ctx, order.ID, h.org, h.user.ID, "order-publish"); err != nil {
		t.Fatalf("publish order: %v", err)
	}
	house := h.household(z, 2, "Lin Family")
	shelterItem := h.openShelter(2)
	if err := h.evac.ConfirmHousehold(ctx, house.ID, shelterItem.ID, h.org, h.user.ID, "confirm"); err != nil {
		t.Fatalf("confirm household: %v", err)
	}
	if err := h.evac.Cancel(ctx, order.ID, h.org, h.user.ID, "cancel-confirmed"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("cancel confirmed order error=%v", err)
	}
	loaded, err := evacuation.NewRepository(h.db).Order(ctx, order.ID)
	if err != nil || loaded.Status != domain.OrderPublished {
		t.Fatalf("order after rejected cancellation=%+v err=%v", loaded, err)
	}
	households, err := evacuation.NewRepository(h.db).Households(ctx, z.ID, string(domain.HouseholdConfirmed))
	if err != nil || len(households) != 1 || households[0].ID != house.ID {
		t.Fatalf("confirmed households=%+v err=%v", households, err)
	}
	if err := h.evac.ConfirmHousehold(ctx, house.ID, shelterItem.ID, h.org, h.user.ID, "confirm-again"); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("repeat confirm error=%v", err)
	}
}

func TestEvacuationRequiresConfirmedZoneAndPublishesOutbox(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	s, err := h.storm.Create(ctx, storm.CreateInput{OrganizationID: h.org, Name: "Unconfirmed", Severity: "orange", ActorID: h.user.ID, RequestID: "unconfirmed"})
	if err != nil {
		t.Fatalf("storm: %v", err)
	}
	z, err := h.storm.AddZone(ctx, s.ID, "Lowland", "medium", h.org, h.user.ID, "zone")
	if err != nil {
		t.Fatalf("zone: %v", err)
	}
	if _, err := h.evac.Create(ctx, s.ID, z.ID, h.org, h.user.ID, "reject-order"); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("unconfirmed zone error=%v", err)
	}
	if err := h.storm.ConfirmZone(ctx, z.ID, h.org, h.user.ID, "confirm-zone"); err != nil {
		t.Fatalf("confirm zone: %v", err)
	}
	order, err := h.evac.Create(ctx, s.ID, z.ID, h.org, h.user.ID, "create-order")
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	if err := h.evac.Publish(ctx, order.ID, h.org, h.user.ID, "publish-order"); err != nil {
		t.Fatalf("publish order: %v", err)
	}
	var eventType string
	if err := h.db.QueryRow(`SELECT event_type FROM outbox_events WHERE aggregate_id=? ORDER BY created_at DESC LIMIT 1`, order.ID).Scan(&eventType); err != nil {
		t.Fatalf("outbox event: %v", err)
	}
	if eventType != "evacuation.published" {
		t.Fatalf("event type=%q", eventType)
	}
}

func TestDispatchLeaseReportEvidenceAndReclaim(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	ctx := context.Background()
	task, err := h.dispatch.Create(ctx, s.ID, z.ID, "Check bridge", 9, h.org, h.user.ID, "task-create")
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	claimed, err := h.dispatch.Claim(ctx, task.ID, "team-a", "claim-a", time.Minute)
	if err != nil || claimed.Status != domain.TaskClaimed || claimed.OwnerID != "team-a" {
		t.Fatalf("claim=%+v err=%v", claimed, err)
	}
	if _, err := h.dispatch.Claim(ctx, task.ID, "team-b", "claim-b", time.Minute); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("second claim error=%v", err)
	}
	if err := h.dispatch.Report(ctx, task.ID, "team-b", "evidence://missing", "Bridge is clear", "wrong-owner"); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("wrong owner report=%v", err)
	}
	if _, err := h.evidence.Put(ctx, "evidence://bridge", strings.NewReader("photo-proof")); err != nil {
		t.Fatalf("put evidence: %v", err)
	}
	if err := h.dispatch.Report(ctx, task.ID, "team-a", "evidence://bridge", "Bridge is clear", "report"); err != nil {
		t.Fatalf("report: %v", err)
	}
	loaded, err := dispatch.NewRepository(h.db).Find(ctx, task.ID)
	if err != nil || loaded.Status != domain.TaskReported || loaded.OwnerID != "" {
		t.Fatalf("reported task=%+v err=%v", loaded, err)
	}
	if err := h.dispatch.Report(ctx, task.ID, "team-a", "evidence://bridge", "again", "repeat"); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("repeat report=%v", err)
	}
	second, err := h.dispatch.Create(ctx, s.ID, z.ID, "Check shelter", 7, h.org, h.user.ID, "task-two")
	if err != nil {
		t.Fatalf("second task: %v", err)
	}
	if _, err := h.dispatch.Claim(ctx, second.ID, "team-c", "claim-c", time.Minute); err != nil {
		t.Fatalf("claim second: %v", err)
	}
	reclaimed, err := h.dispatch.Reclaim(ctx, h.clock.Add(2*time.Hour))
	if err != nil || reclaimed != 1 {
		t.Fatalf("reclaimed=%d err=%v", reclaimed, err)
	}
	queued, err := dispatch.NewRepository(h.db).Queued(ctx, s.ID, 10)
	if err != nil || len(queued) != 1 || queued[0].ID != second.ID {
		t.Fatalf("queued after reclaim=%+v err=%v", queued, err)
	}
}

func TestConcurrentDispatchClaimHasSingleOwner(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	task, err := h.dispatch.Create(context.Background(), s.ID, z.ID, "Joint rescue", 10, h.org, h.user.ID, "joint-task")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	owners := make(chan string, 2)
	var wg sync.WaitGroup
	for _, owner := range []string{"alpha", "bravo"} {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			<-start
			item, err := h.dispatch.Claim(context.Background(), task.ID, owner, "claim-"+owner, time.Hour)
			if err == nil {
				owners <- item.OwnerID
			}
			results <- err
		}(owner)
	}
	close(start)
	wg.Wait()
	close(results)
	close(owners)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		} else if !errors.Is(err, domain.ErrInvalidState) && !errors.Is(err, domain.ErrLeaseLost) && !strings.Contains(strings.ToLower(err.Error()), "locked") {
			t.Fatalf("claim error=%v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("claim winners=%d", wins)
	}
	var owner string
	if err := h.db.QueryRow(`SELECT owner_id FROM dispatch_tasks WHERE id=?`, task.ID).Scan(&owner); err != nil {
		t.Fatalf("owner query: %v", err)
	}
	if owner == "" {
		t.Fatal("claimed task has no owner")
	}
}

func TestCanceledDispatchReportDoesNotChangeTask(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	task, err := h.dispatch.Create(context.Background(), s.ID, z.ID, "Canceled report", 4, h.org, h.user.ID, "cancel-task")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := h.dispatch.Claim(context.Background(), task.ID, "team", "claim", time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := h.evidence.Put(context.Background(), "evidence://cancel", strings.NewReader("proof")); err != nil {
		t.Fatalf("evidence: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = h.dispatch.Report(ctx, task.ID, "team", "evidence://cancel", "a complete report", "cancel-report")
	if err == nil {
		t.Fatal("expected canceled report error")
	}
	loaded, err := dispatch.NewRepository(h.db).Find(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("load task: %v", err)
	}
	if loaded.Status != domain.TaskClaimed || loaded.OwnerID != "team" {
		t.Fatalf("task after canceled report=%+v", loaded)
	}
}

func TestSupplyDispatchDeliveryAndCancellationPreserveInventory(t *testing.T) {
	h := newHarness(t)
	s, _ := h.activeStorm()
	destination := h.openShelter(10)
	ctx := context.Background()
	item, err := h.supply.CreateItem(ctx, h.org, "WATER-500", "Drinking water", 10, h.user.ID, "item")
	if err != nil {
		t.Fatalf("item: %v", err)
	}
	move, err := h.supply.Dispatch(ctx, item.ID, s.ID, destination.ID, h.org, h.user.ID, "move", 4)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if move.Status != domain.MovementInTransit {
		t.Fatalf("movement status=%s", move.Status)
	}
	items, err := h.supply.Inventory(ctx, h.org)
	if err != nil || len(items) != 1 || items[0].Reserved != 4 || items[0].Quantity != 10 {
		t.Fatalf("inventory after dispatch=%+v err=%v", items, err)
	}
	if err := h.supply.Deliver(ctx, move.ID, h.org, h.user.ID, "deliver"); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	items, err = h.supply.Inventory(ctx, h.org)
	if err != nil || items[0].Reserved != 0 || items[0].Quantity != 6 {
		t.Fatalf("inventory after delivery=%+v err=%v", items, err)
	}
	second, err := h.supply.Dispatch(ctx, item.ID, s.ID, destination.ID, h.org, h.user.ID, "move-two", 3)
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	if err := h.supply.Cancel(ctx, second.ID, h.org, h.user.ID, "cancel"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	items, err = h.supply.Inventory(ctx, h.org)
	if err != nil || items[0].Reserved != 0 || items[0].Quantity != 6 {
		t.Fatalf("inventory after cancellation=%+v err=%v", items, err)
	}
	movements, err := supply.NewRepository(h.db).Movements(ctx, s.ID)
	statuses := make([]domain.MovementStatus, 0, len(movements))
	for _, movement := range movements {
		statuses = append(statuses, movement.Status)
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i] < statuses[j] })
	if err != nil || len(movements) != 2 || len(statuses) != 2 || statuses[0] != domain.MovementCancelled || statuses[1] != domain.MovementDelivered {
		t.Fatalf("movements=%+v err=%v", movements, err)
	}
}

func TestSupplyRejectsClosedShelterAndInsufficientInventory(t *testing.T) {
	h := newHarness(t)
	s, _ := h.activeStorm()
	destination := h.openShelter(2)
	item, err := h.supply.CreateItem(context.Background(), h.org, "KIT", "Medical kit", 2, h.user.ID, "item")
	if err != nil {
		t.Fatalf("item: %v", err)
	}
	if _, err := h.supply.Dispatch(context.Background(), item.ID, s.ID, destination.ID, h.org, h.user.ID, "too-much", 3); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("insufficient inventory=%v", err)
	}
	if _, err := h.db.Exec(`UPDATE shelters SET status='closed' WHERE id=?`, destination.ID); err != nil {
		t.Fatalf("close shelter: %v", err)
	}
	if _, err := h.supply.Dispatch(context.Background(), item.ID, s.ID, destination.ID, h.org, h.user.ID, "closed", 1); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("closed shelter=%v", err)
	}
	var reserved int
	if err := h.db.QueryRow(`SELECT reserved FROM supply_items WHERE id=?`, item.ID).Scan(&reserved); err != nil {
		t.Fatalf("reserved: %v", err)
	}
	if reserved != 0 {
		t.Fatalf("reserved after rejected dispatch=%d", reserved)
	}
}

func TestOutboxClaimLeaseFailureAndRecovery(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if err := h.db.Tx(ctx, func(tx *sql.Tx) error {
		return h.outbox.Enqueue(ctx, tx, "storm", "storm-1", "test.event", map[string]string{"value": "one"})
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := h.outbox.Claim(ctx, "worker-a", time.Minute, h.clock)
	if err != nil || claimed.LeaseOwner != "worker-a" || claimed.Attempts != 1 {
		t.Fatalf("claim=%+v err=%v", claimed, err)
	}
	if err := h.outbox.Ack(ctx, claimed.ID, "worker-b"); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("wrong ack=%v", err)
	}
	if err := h.outbox.Fail(ctx, claimed.ID, "worker-a", h.clock.Add(time.Minute), errors.New("remote unavailable")); err != nil {
		t.Fatalf("fail: %v", err)
	}
	claimed, err = h.outbox.Claim(ctx, "worker-b", time.Minute, h.clock.Add(2*time.Minute))
	if err != nil || claimed.LeaseOwner != "worker-b" || claimed.Attempts != 2 {
		t.Fatalf("second claim=%+v err=%v", claimed, err)
	}
	if err := h.outbox.Dead(ctx, claimed.ID, "worker-b", errors.New("permanent")); err != nil {
		t.Fatalf("dead: %v", err)
	}
	var status, owner string
	if err := h.db.QueryRow(`SELECT status,COALESCE(lease_owner,'') FROM outbox_events WHERE id=?`, claimed.ID).Scan(&status, &owner); err != nil {
		t.Fatalf("outbox state: %v", err)
	}
	if status != "dead" || owner != "" {
		t.Fatalf("outbox status=%q owner=%q", status, owner)
	}
}

func TestOutboxRecoveryRequeuesExpiredLeaseAndPurgesOldEvents(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if err := h.db.Tx(ctx, func(tx *sql.Tx) error { return h.outbox.Enqueue(ctx, tx, "storm", "old", "old.event", "payload") }); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := h.outbox.Claim(ctx, "worker", time.Hour, h.clock)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := h.db.Exec(`UPDATE outbox_events SET lease_until=? WHERE id=?`, sqlite.FormatTime(h.clock.Add(-time.Minute)), claimed.ID); err != nil {
		t.Fatalf("age lease: %v", err)
	}
	recovery := outbox.NewRecovery(h.db)
	recovery.Now = func() time.Time { return h.clock }
	expired, err := recovery.Expired(ctx)
	if err != nil || len(expired) != 1 || expired[0].ID != claimed.ID {
		t.Fatalf("expired=%+v err=%v", expired, err)
	}
	if err := recovery.Requeue(ctx, claimed.ID); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	claimed, err = h.outbox.Claim(ctx, "worker-two", time.Minute, h.clock)
	if err != nil || claimed.LeaseOwner != "worker-two" {
		t.Fatalf("reclaimed=%+v err=%v", claimed, err)
	}
	if err := recovery.Terminal(ctx, claimed.ID, errors.New("retained for purge")); err != nil {
		t.Fatalf("terminal: %v", err)
	}
	deleted, err := recovery.Purge(ctx, h.clock.Add(time.Hour))
	if err != nil || deleted < 1 {
		t.Fatalf("purged=%d err=%v", deleted, err)
	}
}

type fakeSender struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (s *fakeSender) Send(_ context.Context, _ alert.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.err
}

func TestWorkerSchedulerCancellationAndOutboxFailureRetry(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	s, z := h.activeStorm()
	order := h.publishedOrder(s, z, "worker-order")
	if _, err := h.alert.EnqueueOrderAlert(ctx, s.ID, order.ID, h.org, h.user.ID, "alert", "webhook"); err != nil {
		t.Fatalf("alert: %v", err)
	}
	cfg := config.Config{WorkerLease: time.Minute, WorkerInterval: time.Millisecond, WebhookURL: "http://127.0.0.1:1"}
	runner := worker.New(h.db, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	runner.Sender = &fakeSender{err: errors.New("remote failed")}
	runner.Tick(ctx)
	var status string
	if err := h.db.QueryRow(`SELECT status FROM outbox_events WHERE event_type='alert.requested' ORDER BY created_at DESC LIMIT 1`).Scan(&status); err != nil {
		t.Fatalf("worker outbox: %v", err)
	}
	if status != "pending" {
		t.Fatalf("failed delivery status=%q, want pending retry", status)
	}
	scheduler := worker.NewScheduler()
	started := make(chan struct{})
	runCtx, stop := context.WithCancel(context.Background())
	if err := scheduler.Register("slow", func(ctx context.Context) error {
		close(started)
		<-runCtx.Done()
		return runCtx.Err()
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := scheduler.Register("slow", func(context.Context) error { return nil }); !errors.Is(err, worker.ErrDuplicateTask) {
		t.Fatalf("duplicate scheduler registration=%v", err)
	}
	go func() { _ = scheduler.Run(runCtx, "slow") }()
	<-started
	if err := scheduler.Run(context.Background(), "slow"); !errors.Is(err, worker.ErrTaskRunning) {
		t.Fatalf("running scheduler error=%v", err)
	}
	stop()
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := scheduler.Run(cancelCtx, "missing"); !errors.Is(err, worker.ErrMissingTask) {
		t.Fatalf("missing scheduler error=%v", err)
	}
}

func TestAuditListAndJSONLExport(t *testing.T) {
	h := newHarness(t)
	s, _ := h.activeStorm()
	ctx := context.Background()
	page, err := h.audit.List(ctx, "storm", s.ID, 20, 0)
	if err != nil {
		t.Fatalf("audit list: %v", err)
	}
	if page.Total < 1 || len(page.Items) < 1 {
		t.Fatalf("audit page=%+v", page)
	}
	var dst bytes.Buffer
	if err := audit.NewExporter(h.db).JSONL(ctx, "storm", s.ID, &dst); err != nil {
		t.Fatalf("export: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(dst.String()), "\n")
	if len(lines) != page.Total {
		t.Fatalf("export lines=%d, audit total=%d", len(lines), page.Total)
	}
	for _, line := range lines {
		var item map[string]any
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			t.Fatalf("invalid audit JSONL %q: %v", line, err)
		}
		event, ok := item["event"].(map[string]any)
		if !ok || event["ObjectID"] != s.ID {
			t.Fatalf("export event=%v", item["event"])
		}
	}
	var canceled bytes.Buffer
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := audit.NewExporter(h.db).JSONL(cancelCtx, "storm", s.ID, &canceled); err == nil {
		t.Fatal("expected canceled export error")
	}
}

func TestQueriesFilterPageAndExport(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	ctx := context.Background()
	first, err := h.dispatch.Create(ctx, s.ID, z.ID, "Priority rescue", 10, h.org, h.user.ID, "q-one")
	if err != nil {
		t.Fatalf("first task: %v", err)
	}
	second, err := h.dispatch.Create(ctx, s.ID, z.ID, "Supply escort", 4, h.org, h.user.ID, "q-two")
	if err != nil {
		t.Fatalf("second task: %v", err)
	}
	page, err := h.query.Tasks(ctx, s.ID, query.TaskFilter{Status: string(domain.TaskQueued), Limit: 1, Offset: 0})
	if err != nil || page.Total != 2 || len(page.Items) != 1 || page.Items[0].ID != first.ID {
		t.Fatalf("task page=%+v err=%v", page, err)
	}
	filtered, err := h.query.Tasks(ctx, s.ID, query.TaskFilter{ZoneID: z.ID, Limit: 20})
	if err != nil || filtered.Total != 2 || len(filtered.Items) != 2 {
		t.Fatalf("filtered tasks=%+v err=%v", filtered, err)
	}
	var csvOut bytes.Buffer
	if err := h.query.ExportTasks(ctx, s.ID, &csvOut); err != nil {
		t.Fatalf("export tasks: %v", err)
	}
	rows, err := csv.NewReader(strings.NewReader(csvOut.String())).ReadAll()
	if err != nil || len(rows) != 3 || rows[0][0] != "id" {
		t.Fatalf("csv rows=%v err=%v", rows, err)
	}
	if rows[1][0] != first.ID || rows[2][0] != second.ID {
		t.Fatalf("csv order=%v", rows)
	}
	storms, err := h.query.StormPage(ctx, h.org, 10, 0)
	if err != nil || storms.Total != 1 || len(storms.Items) != 1 || storms.Items[0].ID != s.ID {
		t.Fatalf("storm query=%+v err=%v", storms, err)
	}
}

func TestStormCloseRequiresAllDependentWorkToReachTerminalState(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	ctx := context.Background()
	order, err := h.evac.Create(ctx, s.ID, z.ID, h.org, h.user.ID, "close-order")
	if err != nil {
		t.Fatalf("order: %v", err)
	}
	if err := h.evac.Publish(ctx, order.ID, h.org, h.user.ID, "publish"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	ready, err := h.storm.CloseReadiness(ctx, s.ID)
	if err != nil || ready {
		t.Fatalf("readiness with open order=%v err=%v", ready, err)
	}
	if err := h.storm.Close(ctx, s.ID, h.user.ID, "close"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("close with open order=%v", err)
	}
	if err := h.evac.Cancel(ctx, order.ID, h.org, h.user.ID, "cancel"); err != nil {
		t.Fatalf("cancel order: %v", err)
	}
	ready, err = h.storm.CloseReadiness(ctx, s.ID)
	if err != nil || !ready {
		t.Fatalf("readiness after cancel=%v err=%v", ready, err)
	}
	if err := h.storm.Close(ctx, s.ID, h.user.ID, "close-final"); err != nil {
		t.Fatalf("close final: %v", err)
	}
	loaded, err := h.storm.Get(ctx, s.ID)
	if err != nil || loaded.Status != domain.StormClosed {
		t.Fatalf("closed storm=%+v err=%v", loaded, err)
	}
	if err := h.storm.Close(ctx, s.ID, h.user.ID, "close-again"); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("close again=%v", err)
	}
}

func TestHTTPHealthReadyAuthenticationAndBusinessErrors(t *testing.T) {
	h := newHarness(t)
	cfg := config.Config{SessionTTL: time.Hour}
	server := httpapi.New(h.db, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	resp, err := ts.Client().Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("health request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status=%d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	resp, err = ts.Client().Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("ready request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ready status=%d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	resp, err = ts.Client().Post(ts.URL+"/v1/storms", "application/json", strings.NewReader(`{"name":"No token","severity":"red"}`))
	if err != nil {
		t.Fatalf("unauthorized request: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	loginBody := strings.NewReader(`{"email":"commander@example.test","password":"secret"}`)
	resp, err = ts.Client().Post(ts.URL+"/v1/auth/login", "application/json", loginBody)
	if err != nil {
		t.Fatalf("login request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login status=%d", resp.StatusCode)
	}
	var login struct{ Token string }
	if err := json.NewDecoder(resp.Body).Decode(&login); err != nil {
		t.Fatalf("login JSON: %v", err)
	}
	_ = resp.Body.Close()
	if login.Token == "" {
		t.Fatal("empty HTTP token")
	}
	request, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/storms", strings.NewReader(`{"name":"HTTP Storm","severity":"red"}`))
	request.Header.Set("Authorization", "Bearer "+login.Token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Request-ID", "http-create")
	resp, err = ts.Client().Do(request)
	if err != nil {
		t.Fatalf("create storm request: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status=%d", resp.StatusCode)
	}
	var created domain.StormEvent
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("storm response: %v", err)
	}
	_ = resp.Body.Close()
	request, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/storms/"+created.ID+"/overview", nil)
	request.Header.Set("Authorization", "Bearer "+login.Token)
	resp, err = ts.Client().Do(request)
	if err != nil {
		t.Fatalf("overview request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("overview status=%d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	request, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/unknown", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer "+login.Token)
	resp, err = ts.Client().Do(request)
	if err != nil {
		t.Fatalf("unknown request: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown status=%d", resp.StatusCode)
	}
	var problem map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&problem); err != nil {
		t.Fatalf("error JSON: %v", err)
	}
	_ = resp.Body.Close()
	if _, ok := problem["error"]; !ok {
		t.Fatalf("error envelope=%v", problem)
	}
}

func TestEvidenceStoreContextAndDigest(t *testing.T) {
	store := evidence.NewMemoryStore()
	ctx := context.Background()
	digest, err := store.Put(ctx, "evidence://one", strings.NewReader("important evidence"))
	if err != nil || digest == "" {
		t.Fatalf("put digest=%q err=%v", digest, err)
	}
	reader, err := store.Open(ctx, "evidence://one")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	actual, err := evidence.Digest(reader)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	if actual != digest {
		t.Fatalf("digest=%q put=%q", actual, digest)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Put(canceled, "evidence://cancel", strings.NewReader("x")); err == nil {
		t.Fatal("expected canceled put")
	}
	if _, err := store.Open(ctx, "evidence://missing"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing evidence error=%v", err)
	}
	if !store.Exists("evidence://one") || store.Exists("evidence://missing") {
		t.Fatal("evidence existence mismatch")
	}
}

func TestDomainTransitionsValidationAndTimeWindows(t *testing.T) {
	if !domain.StormMonitoring.CanMoveTo(domain.StormActive) || domain.StormClosed.CanMoveTo(domain.StormActive) {
		t.Fatal("storm transition table invalid")
	}
	if !domain.OrderDraft.CanMoveTo(domain.OrderPublished) || domain.OrderCancelled.CanMoveTo(domain.OrderPublished) {
		t.Fatal("order transition table invalid")
	}
	if !domain.TaskQueued.CanMoveTo(domain.TaskClaimed) || !domain.TaskClaimed.CanMoveTo(domain.TaskReported) || domain.TaskReported.CanMoveTo(domain.TaskQueued) {
		t.Fatal("task transition table invalid")
	}
	if !domain.RoleCommander.Can("close_storm") || domain.RoleShelter.Can("close_storm") {
		t.Fatal("role rules invalid")
	}
	if err := domain.Required("org", ""); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("required error=%v", err)
	}
	if err := domain.Positive(0); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("positive error=%v", err)
	}
	limit, offset := domain.NormalizePage(-4, -3)
	if limit != 50 || offset != 0 {
		t.Fatalf("normalized page=%d,%d", limit, offset)
	}
	start := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	if !domain.Within(start.Add(30*time.Minute), start, end) || domain.Within(end, start, end) {
		t.Fatal("time window boundary invalid")
	}
	if !domain.IsExpired(end, end) || domain.IsExpired(start, end) {
		t.Fatal("expiry boundary invalid")
	}
	if !domain.ValidSeverity("warning") || domain.ValidSeverity("purple") {
		t.Fatal("severity validation invalid")
	}
}

func TestSQLiteQueryBuilderAndUtilityFunctions(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	q := sqlite.Select("storm_events", "id", "name").Where("organization_id=?", h.org).OrderBy("opened_at DESC").Page(3, 2)
	sqlText, args, err := q.SQL()
	if err != nil {
		t.Fatalf("query SQL: %v", err)
	}
	if !strings.Contains(sqlText, "LIMIT 3 OFFSET 2") || len(args) != 1 || args[0] != h.org {
		t.Fatalf("sql=%q args=%v", sqlText, args)
	}
	rows, err := q.Run(ctx, h.db)
	if err != nil {
		t.Fatalf("run query: %v", err)
	}
	if err := sqlite.RequireRows(rows); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close rows: %v", err)
	}
	if _, _, err := sqlite.Select("", "id").SQL(); err == nil {
		t.Fatal("expected invalid query builder error")
	}
	if sqlite.FormatTime(time.Time{}) != "" || sqlite.TimeOrZero("") != (time.Time{}) {
		t.Fatal("zero time conversion invalid")
	}
	if !sqlite.IsConstraint(fmt.Errorf("UNIQUE constraint failed")) || sqlite.IsConstraint(errors.New("network")) {
		t.Fatal("constraint classifier invalid")
	}
}

func TestRepositorySlicesAreIndependentAndOrderingIsStable(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	ctx := context.Background()
	for _, title := range []string{"Low priority", "High priority", "Medium priority"} {
		priority := 2
		if title == "High priority" {
			priority = 9
		}
		if _, err := h.dispatch.Create(ctx, s.ID, z.ID, title, priority, h.org, h.user.ID, "ordering"); err != nil {
			t.Fatalf("task %s: %v", title, err)
		}
	}
	first, err := h.dispatch.Queue(ctx, s.ID)
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	if len(first) != 3 || first[0].Title != "High priority" {
		t.Fatalf("queue order=%+v", first)
	}
	first[0].Title = "mutated locally"
	second, err := h.dispatch.Queue(ctx, s.ID)
	if err != nil {
		t.Fatalf("queue second: %v", err)
	}
	if second[0].Title == "mutated locally" {
		t.Fatal("repository returned shared mutable state")
	}
	zoneList, err := storm.NewRepository(h.db).Zones(ctx, s.ID)
	if err != nil {
		t.Fatalf("zones: %v", err)
	}
	zoneList[0].Name = "mutated"
	zoneAgain, err := storm.NewRepository(h.db).Zones(ctx, s.ID)
	if err != nil || zoneAgain[0].Name == "mutated" {
		t.Fatalf("zone isolation failed: %+v err=%v", zoneAgain, err)
	}
}

func TestContextCancellationPropagatesToDatabaseCalls(t *testing.T) {
	h := newHarness(t)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.storm.Get(canceled, "storm-missing"); err == nil {
		t.Fatal("expected canceled storm lookup")
	}
	if _, err := h.query.Tasks(canceled, "storm-missing", query.TaskFilter{}); err == nil {
		t.Fatal("expected canceled task lookup")
	}
	if _, err := h.shelter.List(canceled, 10); err == nil {
		t.Fatal("expected canceled shelter lookup")
	}
	if err := h.storm.Close(canceled, "storm-missing", h.user.ID, "cancel-close"); err == nil {
		t.Fatal("expected canceled close")
	}
}

func TestAlertLifecycleAndDeadLetterListing(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	ctx := context.Background()
	order := h.publishedOrder(s, z, "alert-order")
	item, err := h.alert.EnqueueOrderAlert(ctx, s.ID, order.ID, h.org, h.user.ID, "alert-request", "sms")
	if err != nil {
		t.Fatalf("enqueue alert: %v", err)
	}
	found, err := alert.NewRepository(h.db).Find(ctx, item.ID)
	if err != nil || found.Status != "pending" {
		t.Fatalf("found alert=%+v err=%v", found, err)
	}
	due, err := h.alert.Due(ctx, 10)
	if err != nil || len(due) != 1 || due[0].ID != item.ID {
		t.Fatalf("due=%+v err=%v", due, err)
	}
	if err := h.alert.MarkFailed(ctx, item.ID, errors.New("carrier down"), 2); err != nil {
		t.Fatalf("failed alert: %v", err)
	}
	if err := h.alert.MarkFailed(ctx, item.ID, errors.New("carrier down again"), 2); err != nil {
		t.Fatalf("dead alert: %v", err)
	}
	dead, err := alert.NewRepository(h.db).Dead(ctx, s.ID)
	if err != nil || len(dead) != 1 || dead[0].Status != "dead" || dead[0].Attempts != 2 {
		t.Fatalf("dead alerts=%+v err=%v", dead, err)
	}
	if err := h.alert.MarkDelivered(ctx, item.ID); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("delivered dead alert=%v", err)
	}
}

func TestHTTPLoginRejectsMalformedPayloadAndQueryOverviewNotFound(t *testing.T) {
	h := newHarness(t)
	server := httptest.NewServer(httpapi.New(h.db, config.Config{SessionTTL: time.Hour}, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	defer server.Close()
	resp, err := server.Client().Post(server.URL+"/v1/auth/login", "application/json", strings.NewReader("not-json"))
	if err != nil {
		t.Fatalf("malformed login: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed status=%d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	result, err := auth.New(h.db, time.Hour).Login(context.Background(), "commander@example.test", "secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/storms/not-found/overview", nil)
	request.Header.Set("Authorization", "Bearer "+result.Token)
	resp, err = server.Client().Do(request)
	if err != nil {
		t.Fatalf("not found overview: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("not found status=%d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestWorkersStopWhenContextIsCanceled(t *testing.T) {
	h := newHarness(t)
	cfg := config.Config{WorkerInterval: time.Millisecond, WorkerLease: time.Second}
	runner := worker.New(h.db, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runner.Run(ctx)
		close(done)
	}()
	time.Sleep(5 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
	runner.Wait()
}

func TestAuditExportReturnsWriterErrors(t *testing.T) {
	h := newHarness(t)
	s, _ := h.activeStorm()
	writer := failingWriter{}
	err := audit.NewExporter(h.db).JSONL(context.Background(), "storm", s.ID, &writer)
	if err == nil {
		t.Fatal("expected writer error")
	}
}

type failingWriter struct{}

func (*failingWriter) Write([]byte) (int, error) { return 0, errors.New("writer closed") }

func TestQueryExportReturnsWriterError(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	if _, err := h.dispatch.Create(context.Background(), s.ID, z.ID, "Export failure", 1, h.org, h.user.ID, "export"); err != nil {
		t.Fatalf("task: %v", err)
	}
	err := h.query.ExportTasks(context.Background(), s.ID, &failingWriter{})
	if err == nil {
		t.Fatal("expected CSV writer error")
	}
}

func TestServiceErrorsPreserveSentinelCauses(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.storm.Get(ctx, "missing"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("storm not found=%v", err)
	}
	if _, err := h.shelter.Get(ctx, "missing"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("shelter not found=%v", err)
	}
	if _, err := dispatch.NewRepository(h.db).Find(ctx, "missing"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("task not found=%v", err)
	}
	if _, err := supply.NewRepository(h.db).FindItem(ctx, "missing"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("item not found=%v", err)
	}
	if _, err := evacuation.NewRepository(h.db).Order(ctx, "missing"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("order not found=%v", err)
	}
}

func TestMigrationsAreSafeWhenAppliedAgain(t *testing.T) {
	h := newHarness(t)
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	if err := h.db.Migrate(context.Background(), filepath.Join(root, "migrations")); err != nil {
		t.Fatalf("repeat migrate: %v", err)
	}
	var count int
	if err := h.db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("migration count: %v", err)
	}
	if count != 1 {
		t.Fatalf("migration count=%d", count)
	}
}

func TestStormOverviewAggregatesDependentEntities(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	ctx := context.Background()
	order, err := h.evac.Create(ctx, s.ID, z.ID, h.org, h.user.ID, "overview-order")
	if err != nil {
		t.Fatalf("order: %v", err)
	}
	if err := h.evac.Publish(ctx, order.ID, h.org, h.user.ID, "overview-publish"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := h.dispatch.Create(ctx, s.ID, z.ID, "overview-task", 5, h.org, h.user.ID, "overview-task"); err != nil {
		t.Fatalf("task: %v", err)
	}
	shelterItem := h.openShelter(5)
	house := h.household(z, 2, "Overview Family")
	if _, err := h.shelter.Reserve(ctx, shelterItem.ID, house.ID, 2, time.Hour, h.org, h.user.ID, "overview-reserve"); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	item, err := h.supply.CreateItem(ctx, h.org, "OVERVIEW", "Overview goods", 4, h.user.ID, "overview-item")
	if err != nil {
		t.Fatalf("supply: %v", err)
	}
	if _, err := h.supply.Dispatch(ctx, item.ID, s.ID, shelterItem.ID, h.org, h.user.ID, "overview-move", 2); err != nil {
		t.Fatalf("movement: %v", err)
	}
	overview, err := h.query.Overview(ctx, s.ID)
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	if overview.Forecasts != 1 || overview.Zones != 1 || overview.OpenOrders != 1 || overview.OpenTasks != 1 || overview.OccupiedShelterPlaces != 2 || overview.InTransitMovements != 1 {
		t.Fatalf("overview=%+v", overview)
	}
}

func TestAuditDetailsAreStructuredAndRequestBound(t *testing.T) {
	h := newHarness(t)
	s, _ := h.activeStorm()
	page, err := h.audit.List(context.Background(), "storm", s.ID, 50, 0)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if len(page.Items) == 0 {
		t.Fatal("no audit events")
	}
	requests := make([]string, 0, len(page.Items))
	for _, item := range page.Items {
		if item.OrganizationID != h.org || item.ObjectID != s.ID && item.ObjectType == "storm" {
			t.Fatalf("audit ownership/object=%+v", item)
		}
		if item.RequestID == "" || item.Detail == "" {
			t.Fatalf("audit missing request/detail=%+v", item)
		}
		requests = append(requests, item.RequestID)
	}
	sort.Strings(requests)
	if sort.SearchStrings(requests, "req-storm") == len(requests) {
		t.Fatalf("request binding missing: %v", requests)
	}
}
