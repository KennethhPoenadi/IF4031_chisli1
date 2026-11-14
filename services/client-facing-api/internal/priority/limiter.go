package priority

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"client-facing-api/internal/httpx"
)

type Config struct {
	MaxInFlight         int
	ReservedForPriority int
	MaxPriorityQueued   int
	IsPriority          func(*http.Request) bool
}

type Limiter struct {
	mu                sync.Mutex
	maxInFlight       int
	regularCapacity   int
	maxPriorityQueued int
	isPriority        func(*http.Request) bool
	inFlight          int
	priorityQueue     []*waiter
}

type waiter struct {
	ready   chan struct{}
	granted bool
}

type outcome uint8

const (
	admitted outcome = iota
	rejected
	canceled
)

func New(cfg Config) (*Limiter, error) {
	switch {
	case cfg.MaxInFlight < 2:
		return nil, errors.New("max in-flight must be at least 2")
	case cfg.ReservedForPriority < 1:
		return nil, errors.New("reserved priority capacity must be positive")
	case cfg.ReservedForPriority >= cfg.MaxInFlight:
		return nil, errors.New("reserved priority capacity must be less than max in-flight")
	case cfg.MaxPriorityQueued < 1:
		return nil, errors.New("max priority queue must be positive")
	case cfg.IsPriority == nil:
		return nil, errors.New("priority classifier is required")
	}
	return &Limiter{
		maxInFlight:       cfg.MaxInFlight,
		regularCapacity:   cfg.MaxInFlight - cfg.ReservedForPriority,
		maxPriorityQueued: cfg.MaxPriorityQueued,
		isPriority:        cfg.IsPriority,
	}, nil
}

func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		release, result := l.acquire(r.Context(), l.isPriority(r))
		switch result {
		case admitted:
			defer release()
			next.ServeHTTP(w, r)
		case rejected:
			w.Header().Set("Retry-After", "1")
			httpx.WriteError(w, http.StatusTooManyRequests, "too_many_requests", "server is at capacity, retry shortly")
		case canceled:
			return
		}
	})
}

func (l *Limiter) acquire(ctx context.Context, priority bool) (func(), outcome) {
	if ctx.Err() != nil {
		return nil, canceled
	}

	l.mu.Lock()
	capacity := l.regularCapacity
	if priority {
		capacity = l.maxInFlight
	}
	if l.inFlight < capacity && len(l.priorityQueue) == 0 {
		l.inFlight++
		l.mu.Unlock()
		return l.releaseFunc(), admitted
	}
	if !priority || len(l.priorityQueue) >= l.maxPriorityQueued {
		l.mu.Unlock()
		return nil, rejected
	}

	w := &waiter{ready: make(chan struct{})}
	l.priorityQueue = append(l.priorityQueue, w)
	l.mu.Unlock()

	select {
	case <-w.ready:
		if ctx.Err() != nil {
			l.release()
			return nil, canceled
		}
		return l.releaseFunc(), admitted
	case <-ctx.Done():
		l.mu.Lock()
		if !w.granted {
			l.removeWaiterLocked(w)
			l.mu.Unlock()
			return nil, canceled
		}
		l.mu.Unlock()
		l.release()
		return nil, canceled
	}
}

func (l *Limiter) releaseFunc() func() {
	var once sync.Once
	return func() { once.Do(l.release) }
}

func (l *Limiter) release() {
	l.mu.Lock()
	l.inFlight--
	if len(l.priorityQueue) > 0 {
		w := l.priorityQueue[0]
		l.priorityQueue[0] = nil
		l.priorityQueue = l.priorityQueue[1:]
		w.granted = true
		l.inFlight++
		close(w.ready)
	}
	l.mu.Unlock()
}

func (l *Limiter) removeWaiterLocked(target *waiter) {
	for i, queued := range l.priorityQueue {
		if queued == target {
			copy(l.priorityQueue[i:], l.priorityQueue[i+1:])
			l.priorityQueue[len(l.priorityQueue)-1] = nil
			l.priorityQueue = l.priorityQueue[:len(l.priorityQueue)-1]
			return
		}
	}
}
