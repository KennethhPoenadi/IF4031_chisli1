package pvmbg

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

var fixedNow = time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)

func fixedClock() time.Time { return fixedNow }

func hasConfidence(t *testing.T, r VolcanicReport) bool {
	t.Helper()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	_, ok := fields["confidence_level"]
	return ok
}

func TestNewStoreSeedsHistoricalReportsWithTheOriginalSchema(t *testing.T) {
	store := NewStore(20, 4031, fixedClock)
	reports := store.ReportsSince(nil)

	if len(reports) != 20 {
		t.Fatalf("seeded %d reports, the spec requires at least 20 on start", len(reports))
	}
	validVolcanoes := map[string]bool{"G-001": true, "G-002": true, "G-003": true, "G-004": true}
	validLevels := map[string]bool{"Normal": true, "Waspada": true, "Siaga": true, "Awas": true}
	for i, r := range reports {
		if want := fmt.Sprintf("pvmbg-%06d", i+1); r.ReportID != want {
			t.Fatalf("report %d id = %q, want %q", i, r.ReportID, want)
		}
		if !validVolcanoes[r.VolcanoID] || !validLevels[r.AlertLevel] || !r.ReportedAt.Before(fixedNow) {
			t.Fatalf("implausible report: %#v", r)
		}
		if r.EruptionCount24H < 0 || r.EruptionCount24H > 11 || r.AshColumnHeightM < 100 || r.AshColumnHeightM > 5000 {
			t.Fatalf("out of range: %#v", r)
		}
		if hasConfidence(t, r) {
			t.Fatalf("the system must start on the old schema, but %s already has confidence_level", r.ReportID)
		}
	}
}

func TestEnablingTheSchemaIntroducesConfidenceLevelOnlyForNewReports(t *testing.T) {
	store := NewStore(20, 4031, fixedClock)
	if store.SchemaEnabled() {
		t.Fatal("the new field must be off at start")
	}

	store.SetSchemaEnabled(true)
	reports := store.ReportsSince(nil)
	if len(reports) != 21 {
		t.Fatalf("reports = %d, enabling the schema must publish one report immediately", len(reports))
	}
	for _, r := range reports[:20] {
		if hasConfidence(t, r) {
			t.Fatalf("history must not be rewritten: %s has confidence_level", r.ReportID)
		}
	}
	latest := reports[20]
	if !hasConfidence(t, latest) || latest.ConfidenceLevel == nil || *latest.ConfidenceLevel < 0.7 || *latest.ConfidenceLevel > 1.0 {
		t.Fatalf("the new report must carry a confidence_level in [0.7, 1]: %#v", latest)
	}

	store.SetSchemaEnabled(true)
	if got := len(store.ReportsSince(nil)); got != 21 {
		t.Fatalf("enabling twice generated an extra report: %d", got)
	}
}

func TestDisablingTheSchemaStopsAddingTheFieldWithoutTouchingHistory(t *testing.T) {
	store := NewStore(20, 4031, fixedClock)
	store.SetSchemaEnabled(true)
	store.SetSchemaEnabled(false)
	if store.SchemaEnabled() {
		t.Fatal("schema should be disabled")
	}

	after := store.GenerateNow()
	if hasConfidence(t, after) {
		t.Fatal("reports generated after disabling must be back on the old schema")
	}
	reports := store.ReportsSince(nil)
	if len(reports) != 22 || !hasConfidence(t, reports[20]) {
		t.Fatalf("the report made while the schema was on must keep its field; got %d reports", len(reports))
	}
}

func TestOutageFlagToggles(t *testing.T) {
	store := NewStore(20, 4031, fixedClock)
	if store.Outage() {
		t.Fatal("must start healthy")
	}
	store.SetOutage(true)
	if !store.Outage() {
		t.Fatal("outage should be on")
	}
	store.SetOutage(false)
	if store.Outage() {
		t.Fatal("outage should be off again, instantly and without a restart")
	}
}

func TestReportsSinceIsAStrictLowerBound(t *testing.T) {
	store := NewStore(20, 4031, fixedClock)
	reports := store.ReportsSince(nil)

	tenth := reports[9].ReportedAt
	if got := len(store.ReportsSince(&tenth)); got != 10 {
		t.Fatalf("reports after the 10th = %d, want 10", got)
	}
	last := reports[19].ReportedAt
	if got := len(store.ReportsSince(&last)); got != 0 {
		t.Fatalf("reports after the last = %d, want 0", got)
	}
}

func TestRandomDelayStaysInsideTheConfiguredRange(t *testing.T) {
	store := NewStore(20, 4031, fixedClock)
	min, max := 500*time.Millisecond, 3*time.Second

	distinct := map[time.Duration]bool{}
	for i := 0; i < 500; i++ {
		d := store.RandomDelay(min, max)
		if d < min || d > max {
			t.Fatalf("delay %s is outside [%s, %s]", d, min, max)
		}
		distinct[d] = true
	}
	if len(distinct) < 100 {
		t.Fatalf("only %d distinct delays in 500 draws; PVMBG's latency must fluctuate", len(distinct))
	}

	if got := store.RandomDelay(3*time.Second, 3*time.Second); got != 3*time.Second {
		t.Fatalf("a fixed range must return exactly that value, got %s", got)
	}
	if got := store.RandomDelay(2*time.Second, time.Second); got != 2*time.Second {
		t.Fatalf("an inverted range falls back to the minimum, got %s", got)
	}
}

func TestStoreIsDeterministicPerRandomSeed(t *testing.T) {
	a := NewStore(20, 4031, fixedClock).ReportsSince(nil)
	b := NewStore(20, 4031, fixedClock).ReportsSince(nil)
	c := NewStore(20, 999, fixedClock).ReportsSince(nil)

	if !reflect.DeepEqual(a, b) || reflect.DeepEqual(a, c) {
		t.Fatal("the same seed must reproduce the same data, and a different seed must not")
	}
}

func TestNewStoreUsesWallClockWhenNoneIsGiven(t *testing.T) {
	before := time.Now().UTC()
	for _, r := range NewStore(20, 1, nil).ReportsSince(nil) {
		if r.ReportedAt.After(before) {
			t.Fatal("seed reports must be in the past relative to the real clock")
		}
	}
}

func TestRunGeneratorKeepsProducingUntilCancelled(t *testing.T) {
	store := NewStore(20, 4031, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		store.RunGenerator(ctx, 5*time.Millisecond)
		close(done)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for len(store.ReportsSince(nil)) < 23 {
		if time.Now().After(deadline) {
			t.Fatal("the generator did not produce new reports")
		}
		time.Sleep(2 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RunGenerator did not stop")
	}
	settled := len(store.ReportsSince(nil))
	time.Sleep(30 * time.Millisecond)
	if len(store.ReportsSince(nil)) != settled {
		t.Fatal("reports kept appearing after the generator was cancelled")
	}
}

func TestRounding(t *testing.T) {
	if round1(4.449) != 4.4 || round1(4.45) != 4.5 || round2(0.714) != 0.71 || round2(0.715) != 0.72 {
		t.Fatalf("rounding gave %v %v %v %v", round1(4.449), round1(4.45), round2(0.714), round2(0.715))
	}
}
