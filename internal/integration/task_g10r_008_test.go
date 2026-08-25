package integration

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/shelter"
)

func TestShelterReserveRollsBackWhenAuditFails(t *testing.T) {
	h := newHarness(t)
	item := h.openShelter(5)
	if _, err := h.shelter.Reserve(context.Background(), item.ID, "audit-household", 2, 1, "missing-organization", h.user.ID, "reserve-audit-failure"); err == nil {
		t.Fatal("reserve succeeded after audit failure")
	}
	_, reserved, err := shelter.NewRepository(h.db).Capacity(context.Background(), item.ID)
	if err != nil {
		t.Fatalf("capacity: %v", err)
	}
	if reserved != 0 {
		t.Fatalf("reserved=%d after failed reservation", reserved)
	}
	items, err := shelter.NewRepository(h.db).Reservations(context.Background(), item.ID)
	if err != nil {
		t.Fatalf("reservations: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("reservations=%d after failed reservation", len(items))
	}
}
