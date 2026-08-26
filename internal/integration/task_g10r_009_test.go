package integration

import (
	"context"
	"testing"
)

func TestAlertEnqueueRollsBackWhenAuditFails(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	order := h.publishedOrder(s, z, "alert-audit")
	if _, err := h.alert.EnqueueOrderAlert(context.Background(), s.ID, order.ID, "missing-organization", h.user.ID, "alert-audit-failure", "webhook"); err == nil {
		t.Fatal("alert enqueue succeeded after audit failure")
	}
	var alerts int
	if err := h.db.QueryRow(`SELECT COUNT(*) FROM alerts WHERE order_id=?`, order.ID).Scan(&alerts); err != nil {
		t.Fatalf("alerts: %v", err)
	}
	if alerts != 0 {
		t.Fatalf("alerts=%d after failed enqueue", alerts)
	}
}
