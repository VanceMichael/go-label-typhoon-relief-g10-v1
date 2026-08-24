package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
)

func TestSupplyDeliveryRollsBackWhenInventoryReservationIsMissing(t *testing.T) {
	h := newHarness(t)
	s, _ := h.activeStorm()
	destination := h.openShelter(4)
	item, err := h.supply.CreateItem(context.Background(), h.org, "ROLLBACK-KIT", "Rollback kit", 4, h.user.ID, "rollback-item")
	if err != nil {
		t.Fatalf("create item: %v", err)
	}
	move, err := h.supply.Dispatch(context.Background(), item.ID, s.ID, destination.ID, h.org, h.user.ID, "rollback-move", 2)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if _, err := h.db.Exec(`UPDATE supply_items SET reserved=0 WHERE id=?`, item.ID); err != nil {
		t.Fatalf("simulate lost reservation: %v", err)
	}
	if err := h.supply.Deliver(context.Background(), move.ID, h.org, h.user.ID, "rollback-deliver"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("delivery error=%v, want conflict", err)
	}
	var status string
	if err := h.db.QueryRow(`SELECT status FROM supply_movements WHERE id=?`, move.ID).Scan(&status); err != nil {
		t.Fatalf("movement status: %v", err)
	}
	if status != string(domain.MovementInTransit) {
		t.Fatalf("movement status=%q after failed delivery", status)
	}
}
