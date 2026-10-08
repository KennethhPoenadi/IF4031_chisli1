package bmkg

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

var fixedNow = time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)

func fixedClock() time.Time { return fixedNow }

func TestNewStoreSeedsHistoricalEvents(t *testing.T) {
	store := NewStore(20, 4031, fixedClock)
	events := store.EventsSince(nil)

	if len(events) != 20 {
		t.Fatalf("seeded %d events, the spec requires at least 20 on start", len(events))
	}
	for i, e := range events {
		if want := fmt.Sprintf("bmkg-%06d", i+1); e.EventID != want {
			t.Fatalf("event %d id = %q, want %q", i, e.EventID, want)
		}
		if !e.OccurredAt.Before(fixedNow) {
			t.Fatalf("seed event %s occurred at %s, seeds must be historical", e.EventID, e.OccurredAt)
		}
		if i > 0 && e.OccurredAt.Sub(events[i-1].OccurredAt) != time.Minute {
			t.Fatalf("seed events must be one minute apart")
		}
		if e.Magnitude < 4.0 || e.Magnitude > 7.5 || e.DepthKM < 5 || e.DepthKM > 150 ||
			e.EpicenterLat < -11 || e.EpicenterLat > 6 || e.EpicenterLon < 95 || e.EpicenterLon > 141 || e.RegionName == "" {
			t.Fatalf("implausible event: %#v", e)
		}
	}
}

func TestEveryFourthEventHasAMatchingTsunamiWarning(t *testing.T) {
	store := NewStore(20, 4031, fixedClock)
	byID := map[string]SeismicEvent{}
	for _, e := range store.EventsSince(nil) {
		byID[e.EventID] = e
	}

	warnings := store.WarningsSince(nil)
	if len(warnings) != 5 {
		t.Fatalf("warnings = %d, want 5 (events 4, 8, 12, 16, 20)", len(warnings))
	}
	related := map[string]bool{}
	levels := map[string]bool{}
	for _, w := range warnings {
		event, ok := byID[w.RelatedEventID]
		if !ok || !event.PotentialTsunami {
			t.Fatalf("warning %s references %q which is not a potential-tsunami event", w.WarningID, w.RelatedEventID)
		}
		if !reflect.DeepEqual(w.AffectedZones, []string{event.RegionName}) || w.EstimatedArrival.Sub(event.OccurredAt) != 45*time.Minute {
			t.Fatalf("warning %#v does not match its event %#v", w, event)
		}
		related[w.RelatedEventID] = true
		levels[w.ThreatLevel] = true
	}
	for _, e := range byID {
		if e.PotentialTsunami != related[e.EventID] {
			t.Fatalf("event %s potential_tsunami=%v but has-warning=%v; warnings exist only for tsunami events", e.EventID, e.PotentialTsunami, related[e.EventID])
		}
	}
	for _, level := range []string{"Waspada", "Siaga", "Awas"} {
		if !levels[level] {
			t.Fatalf("threat level %q never appears; demo data must cover every level, got %v", level, levels)
		}
	}
}

func TestSinceIsAStrictLowerBound(t *testing.T) {
	store := NewStore(20, 4031, fixedClock)
	events := store.EventsSince(nil)

	tenth := events[9].OccurredAt
	if got := len(store.EventsSince(&tenth)); got != 10 {
		t.Fatalf("events after the 10th = %d, want 10 (the boundary event itself is excluded)", got)
	}
	last := events[19].OccurredAt
	if got := len(store.EventsSince(&last)); got != 0 {
		t.Fatalf("events after the last one = %d, want 0", got)
	}

	before := events[0].OccurredAt.Add(-time.Hour)
	if got := len(store.WarningsSince(&before)); got != 5 {
		t.Fatalf("warnings since long ago = %d, want 5", got)
	}
	if got := len(store.WarningsSince(&fixedNow)); got != 0 {
		t.Fatalf("warnings in the future = %d, want 0", got)
	}
}

func TestGenerateNowAddsAnEventAndItsWarning(t *testing.T) {
	store := NewStore(20, 4031, fixedClock)

	var last SeismicEvent
	for i := 0; i < 4; i++ {
		last = store.GenerateNow()
	}

	if last.EventID != "bmkg-000024" || !last.OccurredAt.Equal(fixedNow) {
		t.Fatalf("event = %#v", last)
	}
	if !last.PotentialTsunami {
		t.Fatal("the 24th event is a multiple of four and must carry a tsunami warning")
	}
	if len(store.EventsSince(nil)) != 24 || len(store.WarningsSince(nil)) != 6 {
		t.Fatalf("events=%d warnings=%d", len(store.EventsSince(nil)), len(store.WarningsSince(nil)))
	}
}

func TestStoreIsDeterministicPerRandomSeed(t *testing.T) {
	a := NewStore(20, 4031, fixedClock).EventsSince(nil)
	b := NewStore(20, 4031, fixedClock).EventsSince(nil)
	c := NewStore(20, 999, fixedClock).EventsSince(nil)

	if !reflect.DeepEqual(a, b) {
		t.Fatal("the same seed must reproduce the same data so demos are repeatable")
	}
	if reflect.DeepEqual(a, c) {
		t.Fatal("a different seed must produce different data")
	}
}

func TestNewStoreUsesWallClockWhenNoneIsGiven(t *testing.T) {
	before := time.Now().UTC()
	store := NewStore(20, 1, nil)
	for _, e := range store.EventsSince(nil) {
		if e.OccurredAt.After(before) {
			t.Fatal("seed events must be in the past relative to the real clock")
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
	for len(store.EventsSince(nil)) < 23 {
		if time.Now().After(deadline) {
			t.Fatal("the generator did not produce new events")
		}
		time.Sleep(2 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RunGenerator did not stop")
	}
	settled := len(store.EventsSince(nil))
	time.Sleep(30 * time.Millisecond)
	if len(store.EventsSince(nil)) != settled {
		t.Fatal("events kept appearing after the generator was cancelled")
	}
}

func TestRounding(t *testing.T) {
	if round1(4.449) != 4.4 || round1(4.45) != 4.5 || round4(1.23456) != 1.2346 {
		t.Fatalf("round1/round4 gave %v %v %v", round1(4.449), round1(4.45), round4(1.23456))
	}
}
