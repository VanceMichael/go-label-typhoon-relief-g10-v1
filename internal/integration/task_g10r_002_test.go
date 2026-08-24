package integration

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
)

func TestConcurrentOutboxClaimHasSingleOwner(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if err := h.db.Tx(ctx, func(tx *sql.Tx) error {
		return h.outbox.Enqueue(ctx, tx, "storm", "storm-claim", "forecast.published", "payload")
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, owner := range []string{"alpha", "bravo"} {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			<-start
			_, err := h.outbox.Claim(ctx, owner, 1, h.clock)
			results <- err
		}(owner)
	}
	close(start)
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		} else if !errors.Is(err, domain.ErrLeaseLost) && !strings.Contains(strings.ToLower(err.Error()), "locked") {
			t.Fatalf("claim error=%v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("claim winners=%d, want one", wins)
	}
}
