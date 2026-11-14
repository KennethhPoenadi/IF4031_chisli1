package priority

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type admissionResult struct {
	name    string
	release func()
	outcome outcome
}

func testLimiter(t *testing.T, max, reserved, queued int) *Limiter {
	t.Helper()
	l, err := New(Config{
		MaxInFlight: max, ReservedForPriority: reserved, MaxPriorityQueued: queued,
		IsPriority: func(r *http.Request) bool { return r.Header.Get("X-Test-Priority") == "true" },
	})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	classifier := func(*http.Request) bool { return false }
	tests := []Config{
		{MaxInFlight: 1, ReservedForPriority: 1, MaxPriorityQueued: 1, IsPriority: classifier},
		{MaxInFlight: 2, ReservedForPriority: 0, MaxPriorityQueued: 1, IsPriority: classifier},
		{MaxInFlight: 2, ReservedForPriority: 2, MaxPriorityQueued: 1, IsPriority: classifier},
		{MaxInFlight: 2, ReservedForPriority: 1, MaxPriorityQueued: 0, IsPriority: classifier},
		{MaxInFlight: 2, ReservedForPriority: 1, MaxPriorityQueued: 1},
	}
	for i, cfg := range tests {
		if _, err := New(cfg); err == nil {
			t.Fatalf("case %d accepted invalid configuration", i)
		}
	}
}

func TestRegularRequestsCannotConsumeReservedCapacity(t *testing.T) {
	l := testLimiter(t, 3, 1, 2)
	releaseOne, got := l.acquire(context.Background(), false)
	if got != admitted {
		t.Fatalf("first regular request = %v", got)
	}
	defer releaseOne()
	releaseTwo, got := l.acquire(context.Background(), false)
	if got != admitted {
		t.Fatalf("second regular request = %v", got)
	}
	defer releaseTwo()
	if _, got := l.acquire(context.Background(), false); got != rejected {
		t.Fatalf("regular request beyond shared capacity = %v, want rejected", got)
	}

	releasePriority, got := l.acquire(context.Background(), true)
	if got != admitted {
		t.Fatalf("priority request did not use reserved capacity: %v", got)
	}
	releasePriority()
}

func TestQueuedPriorityRequestsAreAdmittedFIFO(t *testing.T) {
	l := testLimiter(t, 2, 1, 2)
	releaseRegular, _ := l.acquire(context.Background(), false)
	releasePriority, _ := l.acquire(context.Background(), true)
	defer releasePriority()

	results := make(chan admissionResult, 2)
	for _, name := range []string{"first", "second"} {
		name := name
		go func() {
			release, got := l.acquire(context.Background(), true)
			results <- admissionResult{name: name, release: release, outcome: got}
		}()
		waitForQueueLength(t, l, map[string]int{"first": 1, "second": 2}[name])
	}

	releaseRegular()
	first := receiveResult(t, results)
	if first.name != "first" || first.outcome != admitted {
		t.Fatalf("first handoff = %#v", first)
	}
	first.release()
	second := receiveResult(t, results)
	if second.name != "second" || second.outcome != admitted {
		t.Fatalf("second handoff = %#v", second)
	}
	second.release()
}

func TestPriorityQueueIsBoundedAndCancellationRemovesWaiter(t *testing.T) {
	l := testLimiter(t, 2, 1, 1)
	releaseRegular, _ := l.acquire(context.Background(), false)
	releasePriority, _ := l.acquire(context.Background(), true)
	defer releaseRegular()
	defer releasePriority()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan outcome, 1)
	go func() {
		_, got := l.acquire(ctx, true)
		result <- got
	}()
	waitForQueueLength(t, l, 1)
	if _, got := l.acquire(context.Background(), true); got != rejected {
		t.Fatalf("request beyond queue limit = %v, want rejected", got)
	}

	cancel()
	if got := receiveOutcome(t, result); got != canceled {
		t.Fatalf("canceled waiter outcome = %v", got)
	}
	waitForQueueLength(t, l, 0)
}

func TestMiddlewareReturnsControlled429AndLetsPriorityUseReserve(t *testing.T) {
	l := testLimiter(t, 2, 1, 1)
	releaseRegular, got := l.acquire(context.Background(), false)
	if got != admitted {
		t.Fatal("failed to occupy regular capacity")
	}
	defer releaseRegular()

	handler := l.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	regular := httptest.NewRecorder()
	handler.ServeHTTP(regular, httptest.NewRequest(http.MethodGet, "/", nil))
	if regular.Code != http.StatusTooManyRequests || regular.Header().Get("Retry-After") != "1" {
		t.Fatalf("regular response = %d headers=%v", regular.Code, regular.Header())
	}

	priorityRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	priorityRequest.Header.Set("X-Test-Priority", "true")
	priorityResponse := httptest.NewRecorder()
	handler.ServeHTTP(priorityResponse, priorityRequest)
	if priorityResponse.Code != http.StatusNoContent {
		t.Fatalf("priority response = %d, want 204", priorityResponse.Code)
	}
}

func waitForQueueLength(t *testing.T, l *Limiter, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		l.mu.Lock()
		got := len(l.priorityQueue)
		l.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("priority queue length did not become %d", want)
}

func receiveResult(t *testing.T, results <-chan admissionResult) admissionResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for admission result")
		return admissionResult{}
	}
}

func receiveOutcome(t *testing.T, results <-chan outcome) outcome {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for admission outcome")
		return rejected
	}
}
