package consumer

import (
	"sync"
	"testing"
	"time"

	"dashboard-updater/internal/hazard"
)

func TestSnapshotOfAnEmptyState(t *testing.T) {
	snap := NewState().Snapshot(5)
	if snap.Total != 0 || len(snap.Latest) != 0 || len(snap.BySeverity) != 0 || len(snap.ByType) != 0 {
		t.Fatalf("snapshot = %#v", snap)
	}
	if snap.BySeverity == nil || snap.ByType == nil {
		t.Fatal("the maps must be non-nil so they serialise as {} rather than null")
	}
}

func TestSnapshotCountsAndOrdersTheLatestEvents(t *testing.T) {
	state := NewState()
	base := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	for i, tc := range []struct {
		id       string
		typ      hazard.Type
		severity hazard.Severity
	}{
		{"hz-old", hazard.TypeSeismic, hazard.SeverityNormal},
		{"hz-mid", hazard.TypeVolcanic, hazard.SeverityAwas},
		{"hz-new", hazard.TypeSeismic, hazard.SeverityAwas},
	} {
		ev := event(tc.id, tc.severity)
		ev.HazardType, ev.OccurredAt = tc.typ, base.Add(time.Duration(i)*time.Hour)
		state.Apply(ev)
	}

	snap := state.Snapshot(2)
	if snap.Total != 3 || snap.BySeverity[hazard.SeverityAwas] != 2 || snap.BySeverity[hazard.SeverityNormal] != 1 {
		t.Fatalf("totals wrong: %#v", snap)
	}
	if snap.ByType[hazard.TypeSeismic] != 2 || snap.ByType[hazard.TypeVolcanic] != 1 {
		t.Fatalf("by type wrong: %#v", snap.ByType)
	}
	if len(snap.Latest) != 2 || snap.Latest[0].HazardID != "hz-new" || snap.Latest[1].HazardID != "hz-mid" {
		t.Fatalf("latest must be the newest first and trimmed to the limit: %#v", snap.Latest)
	}
	if snap.Latest[0].ReceivedAt.IsZero() {
		t.Fatal("received_at must be stamped")
	}
}

func TestApplyOverwritesTheSameHazard(t *testing.T) {
	state := NewState()
	state.Apply(event("hz-1", hazard.SeverityWaspada))
	state.Apply(event("hz-1", hazard.SeverityAwas))

	snap := state.Snapshot(10)
	if snap.Total != 1 || snap.Latest[0].Severity != hazard.SeverityAwas {
		t.Fatalf("snapshot = %#v", snap)
	}
}

func TestStateIsSafeForConcurrentUse(t *testing.T) {
	state := NewState()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			state.Apply(event(time.Now().Format("150405.000000")+string(rune('a'+i)), hazard.SeverityNormal))
		}(i)
		go func() {
			defer wg.Done()
			state.Snapshot(5)
		}()
	}
	wg.Wait()
}
