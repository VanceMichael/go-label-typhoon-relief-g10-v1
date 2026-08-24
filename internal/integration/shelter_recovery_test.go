package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/shelter"
)

func TestConcurrentShelterExpiryReleasesCapacityOnlyOnce(t *testing.T) {
	h := newHarness(t)
	item := h.openShelter(3)
	if _, err := h.shelter.Reserve(context.Background(), item.ID, "expiry-house", 2, 1, h.org, h.user.ID, "expiry"); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	now := h.clock.Add(time.Hour)
	results := make(chan int, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			released, err := h.shelter.ReleaseExpired(context.Background(), now)
			if err != nil {
				t.Errorf("release expired: %v", err)
			}
			results <- released
		}()
	}
	wg.Wait()
	close(results)
	total := 0
	for released := range results {
		total += released
	}
	if total != 1 {
		t.Fatalf("released total=%d, want one", total)
	}
	_, reserved, err := shelter.NewRepository(h.db).Capacity(context.Background(), item.ID)
	if err != nil {
		t.Fatalf("capacity: %v", err)
	}
	if reserved != 0 {
		t.Fatalf("reserved=%d after concurrent expiry", reserved)
	}
}
