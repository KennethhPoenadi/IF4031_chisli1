package auth

import (
	"sync"
	"time"
)

type loginThrottle struct {
	mu       sync.Mutex
	max      int
	window   time.Duration
	failures map[string][]time.Time
	now      func() time.Time
}

func newLoginThrottle(max int, window time.Duration) *loginThrottle {
	return &loginThrottle{max: max, window: window, failures: map[string][]time.Time{}, now: time.Now}
}

func (t *loginThrottle) pruneLocked(key string) []time.Time {
	cutoff := t.now().Add(-t.window)
	kept := t.failures[key][:0]
	for _, at := range t.failures[key] {
		if at.After(cutoff) {
			kept = append(kept, at)
		}
	}
	if len(kept) == 0 {
		delete(t.failures, key)
		return nil
	}
	t.failures[key] = kept
	return kept
}

func (t *loginThrottle) blocked(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pruneLocked(key)) >= t.max
}

func (t *loginThrottle) fail(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pruneLocked(key)
	t.failures[key] = append(t.failures[key], t.now())
}

func (t *loginThrottle) reset(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.failures, key)
}
