package integration

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/dispatch"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
)

func TestDispatchReportRejectsMissingEvidence(t *testing.T) {
	h := newHarness(t)
	s, z := h.activeStorm()
	task, err := h.dispatch.Create(context.Background(), s.ID, z.ID, "Bridge inspection", 8, h.org, h.user.ID, "evidence-task")
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if _, err := h.dispatch.Claim(context.Background(), task.ID, "team-a", "evidence-claim", 1); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if err := h.dispatch.Report(context.Background(), task.ID, "team-a", "evidence://not-uploaded", "bridge checked", "evidence-report"); err == nil {
		t.Fatal("report succeeded without evidence")
	}
	loaded, err := dispatch.NewRepository(h.db).Find(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("load task: %v", err)
	}
	if loaded.Status != domain.TaskClaimed || loaded.OwnerID != "team-a" {
		t.Fatalf("task=%+v after missing evidence", loaded)
	}
}
