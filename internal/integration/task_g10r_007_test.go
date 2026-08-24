package integration

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/audit"
)

type cancelExportWriter struct {
	cancel context.CancelFunc
	writes int
}

func (w *cancelExportWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == 1 {
		w.cancel()
	}
	return len(p), nil
}

func TestAuditExportReturnsCancellation(t *testing.T) {
	h := newHarness(t)
	s, _ := h.activeStorm()
	ctx, cancel := context.WithCancel(context.Background())
	dst := &cancelExportWriter{cancel: cancel}
	err := audit.NewExporter(h.db).JSONL(ctx, "storm", s.ID, dst)
	if err == nil {
		t.Fatal("audit export succeeded after context cancellation")
	}
	if dst.writes != 1 {
		t.Fatalf("writes=%d after cancellation, want one", dst.writes)
	}
}
