package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storm"
)

func TestRiskZoneCreationRollsBackWhenAuditWriteFails(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	s, err := h.storm.Create(ctx, storm.CreateInput{OrganizationID: h.org, Name: "North ridge", Severity: "red", ActorID: h.user.ID, RequestID: "north-create"})
	if err != nil {
		t.Fatalf("storm: %v", err)
	}
	// An audit organization_id that does not reference a real organization fails
	// the audit_events foreign key. The risk zone insert must roll back so no
	// boundary is left behind for later confirmation.
	if _, err := h.storm.AddZone(ctx, s.ID, "North ridge", "high", "missing-org", h.user.ID, "north-zone"); err == nil {
		t.Fatal("expected audit failure to reject zone creation")
	}
	var zones int
	if err := h.db.QueryRow(`SELECT COUNT(*) FROM risk_zones WHERE storm_id=? AND name=?`, s.ID, "North ridge").Scan(&zones); err != nil {
		t.Fatalf("query zones: %v", err)
	}
	if zones != 0 {
		t.Fatalf("rolled-back zone count=%d", zones)
	}
	var audits int
	if err := h.db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE request_id=?`, "north-zone").Scan(&audits); err != nil {
		t.Fatalf("query audit: %v", err)
	}
	if audits != 0 {
		t.Fatalf("rolled-back audits=%d", audits)
	}
	// The storm remains usable: a valid zone can still be added and confirmed.
	z, err := h.storm.AddZone(ctx, s.ID, "Ridge", "high", h.org, h.user.ID, "north-zone-retry")
	if err != nil {
		t.Fatalf("retry zone: %v", err)
	}
	if err := h.storm.ConfirmZone(ctx, z.ID, h.org, h.user.ID, "north-zone-confirm"); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	var confirmed int
	if err := h.db.QueryRow(`SELECT confirmed FROM risk_zones WHERE id=?`, z.ID).Scan(&confirmed); err != nil {
		t.Fatalf("load zone: %v", err)
	}
	if confirmed != 1 {
		t.Fatalf("zone confirmed=%d", confirmed)
	}
	// Sentinel classification still surfaces for genuine constraint violations.
	if _, err := h.storm.AddZone(ctx, s.ID, "Ridge", "high", h.org, h.user.ID, "north-zone-retry"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("duplicate zone error=%v", err)
	}
}
