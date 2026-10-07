package httpx

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLimitRejectsOverflowAndReleasesSlots(t *testing.T) {
	const capacity = 2

	entered := make(chan struct{}, capacity)
	release := make(chan struct{})
	var handlerCalls atomic.Int32
	handler := Limit(capacity)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if handlerCalls.Add(1) <= capacity {
			entered <- struct{}{}
			<-release
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	type result struct {
		status     int
		retryAfter string
	}
	results := make(chan result, capacity)
	var workers sync.WaitGroup
	workers.Add(capacity)
	for i := 0; i < capacity; i++ {
		go func() {
			defer workers.Done()
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
			results <- result{status: recorder.Code, retryAfter: recorder.Header().Get("Retry-After")}
		}()
	}

	for i := 0; i < capacity; i++ {
		<-entered
	}

	overflow := httptest.NewRecorder()
	handler.ServeHTTP(overflow, httptest.NewRequest(http.MethodGet, "/", nil))
	if overflow.Code != http.StatusTooManyRequests {
		t.Fatalf("overflow status = %d, want %d", overflow.Code, http.StatusTooManyRequests)
	}
	if got := overflow.Header().Get("Retry-After"); got != "1" {
		t.Errorf("overflow Retry-After = %q, want %q", got, "1")
	}

	close(release)
	workers.Wait()
	close(results)
	for got := range results {
		if got.status != http.StatusNoContent {
			t.Errorf("admitted request status = %d, want %d", got.status, http.StatusNoContent)
		}
		if got.retryAfter != "" {
			t.Errorf("admitted request Retry-After = %q, want empty", got.retryAfter)
		}
	}

	after := httptest.NewRecorder()
	handler.ServeHTTP(after, httptest.NewRequest(http.MethodGet, "/", nil))
	if after.Code != http.StatusNoContent {
		t.Fatalf("request after release status = %d, want %d", after.Code, http.StatusNoContent)
	}
}
