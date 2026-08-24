package integration

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
)

func TestEvacuationCancelRollsBackWhenAuditFails(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	order, err := h.evac.Create(context.Background(), s.ID, z.ID, h.org, h.user.ID, "cancel-audit-create")
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	if err := h.evac.Publish(context.Background(), order.ID, h.org, h.user.ID, "cancel-audit-publish"); err != nil {
		t.Fatalf("publish order: %v", err)
	}
	cancelErr := h.evac.Cancel(context.Background(), order.ID, "missing-organization", h.user.ID, "cancel-audit-failure")
	if cancelErr == nil {
		t.Fatal("cancel succeeded after audit failure")
	}
	var status string
	if err := h.db.QueryRow(`SELECT status FROM evacuation_orders WHERE id=?`, order.ID).Scan(&status); err != nil {
		t.Fatalf("load order: %v", err)
	}
	if status != string(domain.OrderPublished) {
		t.Fatalf("order status=%q after failed cancellation", status)
	}
}
