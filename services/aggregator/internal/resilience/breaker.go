package resilience

import (
	"errors"
	"sync"
	"time"
)

var ErrOpen = errors.New("circuit breaker is open")

type State uint8

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type Config struct {
	FailureThreshold int
	OpenDuration     time.Duration
	Now              func() time.Time
}

type Breaker struct {
	mu            sync.Mutex
	threshold     int
	openDuration  time.Duration
	now           func() time.Time
	state         State
	failures      int
	openedAt      time.Time
	probeInFlight bool
	generation    uint64
}

type Permit struct {
	breaker    *Breaker
	generation uint64
	once       sync.Once
}

func New(cfg Config) *Breaker {
	if cfg.FailureThreshold < 1 {
		cfg.FailureThreshold = 3
	}
	if cfg.OpenDuration <= 0 {
		cfg.OpenDuration = 10 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Breaker{threshold: cfg.FailureThreshold, openDuration: cfg.OpenDuration, now: cfg.Now}
}

func (b *Breaker) Allow() (*Permit, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance()
	switch b.state {
	case Open:
		return nil, ErrOpen
	case HalfOpen:
		if b.probeInFlight {
			return nil, ErrOpen
		}
		b.probeInFlight = true
	}
	return &Permit{breaker: b, generation: b.generation}, nil
}

func (p *Permit) Success() {
	p.complete(true)
}

func (p *Permit) Failure() {
	p.complete(false)
}

func (p *Permit) complete(success bool) {
	if p == nil || p.breaker == nil {
		return
	}
	p.once.Do(func() {
		p.breaker.complete(p.generation, success)
	})
}

func (b *Breaker) complete(generation uint64, success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if generation != b.generation || b.state == Open {
		return
	}
	if success {
		b.state, b.failures, b.probeInFlight = Closed, 0, false
		return
	}
	if b.state == HalfOpen || b.failures+1 >= b.threshold {
		b.open()
		return
	}
	b.failures++
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance()
	return b.state
}

func (b *Breaker) advance() {
	if b.state == Open && !b.now().Before(b.openedAt.Add(b.openDuration)) {
		b.state, b.probeInFlight = HalfOpen, false
		b.generation++
	}
}

func (b *Breaker) open() {
	b.state, b.openedAt, b.probeInFlight = Open, b.now(), false
	b.generation++
}
