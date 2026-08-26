package integration

import (
	"context"
	"testing"
)

func TestRiskZoneCreateRollsBackWhenAuditFails(t *testing.T) {
	h := newHarness(t)
	s, _ := h.activeStorm()
	if _, err := h.storm.AddZone(context.Background(), s.ID, "North ridge", "high", "missing-organization", h.user.ID, "zone-audit-failure"); err == nil {
		t.Fatal("zone creation succeeded after audit failure")
	}
	var zones int
	if err := h.db.QueryRow(`SELECT COUNT(*) FROM risk_zones WHERE storm_id=? AND name=?`, s.ID, "North ridge").Scan(&zones); err != nil {
		t.Fatalf("zones: %v", err)
	}
	if zones != 0 {
		t.Fatalf("zones=%d after failed zone creation", zones)
	}
}
