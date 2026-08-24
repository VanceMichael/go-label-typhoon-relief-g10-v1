package integration

import (
	"context"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storm"
)

func TestForecastPublishRollsBackWhenAuditFails(t *testing.T) {
	h := newHarness(t)
	stormItem, err := h.storm.Create(context.Background(), storm.CreateInput{OrganizationID: h.org, Name: "Forecast audit boundary", Severity: "red", ActorID: h.user.ID, RequestID: "forecast-storm"})
	if err != nil {
		t.Fatalf("create storm: %v", err)
	}
	_, err = h.storm.PublishForecast(context.Background(), storm.ForecastInput{StormID: stormItem.ID, Source: "regional-center", WarningLevel: "red", WindKPH: 120, ValidUntil: h.clock.Add(time.Hour), OrganizationID: "missing-organization", ActorID: h.user.ID, RequestID: "forecast-audit-failure"})
	if err == nil {
		t.Fatal("forecast publish succeeded after its audit write failed")
	}
	var forecasts int
	if err := h.db.QueryRow(`SELECT COUNT(*) FROM forecast_versions WHERE storm_id=?`, stormItem.ID).Scan(&forecasts); err != nil {
		t.Fatalf("forecast count: %v", err)
	}
	if forecasts != 0 {
		t.Fatalf("forecast rows=%d after failed audit, want rollback", forecasts)
	}
}
