package resilience

import (
	"errors"
	"testing"
	"time"
)

func TestBreakerClosedOpenHalfOpenRecovery(t *testing.T) {
	now := time.Unix(100, 0)
	b := New(Config{FailureThreshold: 2, OpenDuration: time.Second, Now: func() time.Time { return now }})
	permit, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	permit.Failure()
	if b.State() != Closed {
		t.Fatalf("state = %v, want closed", b.State())
	}
	permit, err = b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	permit.Failure()
	if b.State() != Open {
		t.Fatalf("state = %v, want open", b.State())
	}
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow error = %v", err)
	}
	now = now.Add(time.Second)
	if b.State() != HalfOpen {
		t.Fatalf("state = %v, want half-open", b.State())
	}
	probe, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("second probe error = %v", err)
	}
	probe.Success()
	if b.State() != Closed {
		t.Fatalf("state = %v, want closed", b.State())
	}
}

func TestHalfOpenFailureReopens(t *testing.T) {
	now := time.Unix(100, 0)
	b := New(Config{FailureThreshold: 1, OpenDuration: time.Second, Now: func() time.Time { return now }})
	permit, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	permit.Failure()
	now = now.Add(time.Second)
	probe, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	probe.Failure()
	if b.State() != Open {
		t.Fatalf("state = %v, want open", b.State())
	}
}

func TestLateOutcomeCannotOverrideNewGeneration(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(Config{FailureThreshold: 1, OpenDuration: 10 * time.Second, Now: func() time.Time { return now }})
	lateSuccess, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	lateFailure, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	failing, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	failing.Failure()
	lateSuccess.Success()
	if b.State() != Open {
		t.Fatalf("late success changed state to %v", b.State())
	}

	now = now.Add(5 * time.Second)
	lateFailure.Failure()
	now = now.Add(6 * time.Second)
	if b.State() != HalfOpen {
		t.Fatalf("late failure extended open interval; state = %v", b.State())
	}
}

func TestPermitAppliesOnlyFirstOutcome(t *testing.T) {
	b := New(Config{FailureThreshold: 1})
	permit, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	permit.Success()
	permit.Failure()
	if b.State() != Closed {
		t.Fatalf("second outcome changed state to %v", b.State())
	}
}

func TestStateString(t *testing.T) {
	for state, want := range map[State]string{Closed: "closed", Open: "open", HalfOpen: "half-open", State(99): "unknown"} {
		if got := state.String(); got != want {
			t.Errorf("State(%d).String() = %q; want %q", state, got, want)
		}
	}
}
