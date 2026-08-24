package integration

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
)

func TestSupplyCancelRollsBackWhenAuditFails(t *testing.T) {
	h := newHarness(t)
	s, _ := h.activeStorm()
	destination := h.openShelter(4)
	item, err := h.supply.CreateItem(context.Background(), h.org, "AUDIT-KIT", "Audit kit", 4, h.user.ID, "audit-item")
	if err != nil {
		t.Fatalf("create item: %v", err)
	}
	move, err := h.supply.Dispatch(context.Background(), item.ID, s.ID, destination.ID, h.org, h.user.ID, "audit-move", 2)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if err := h.supply.Cancel(context.Background(), move.ID, "missing-organization", h.user.ID, "audit-cancel"); err == nil {
		t.Fatal("cancel succeeded after audit failure")
	}
	var status string
	if err := h.db.QueryRow(`SELECT status FROM supply_movements WHERE id=?`, move.ID).Scan(&status); err != nil {
		t.Fatalf("movement: %v", err)
	}
	if status != string(domain.MovementInTransit) {
		t.Fatalf("movement status=%q after failed cancel", status)
	}
	var reserved int
	if err := h.db.QueryRow(`SELECT reserved FROM supply_items WHERE id=?`, item.ID).Scan(&reserved); err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if reserved != 2 {
		t.Fatalf("reserved=%d after failed cancel, want 2", reserved)
	}
}
