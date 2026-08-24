package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
)

func TestStormCloseBlocksQueuedDispatch(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	if _, err := h.dispatch.Create(context.Background(), s.ID, z.ID, "Queued bridge patrol", 9, h.org, h.user.ID, "queued-close"); err != nil {
		t.Fatalf("create task: %v", err)
	}
	err := h.storm.Close(context.Background(), s.ID, h.user.ID, "close-queued")
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("close queued storm=%v, want conflict", err)
	}
	loaded, err := h.storm.Get(context.Background(), s.ID)
	if err != nil {
		t.Fatalf("load storm: %v", err)
	}
	if loaded.Status != domain.StormActive {
		t.Fatalf("storm status=%q after rejected close", loaded.Status)
	}
}
