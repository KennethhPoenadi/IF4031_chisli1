package leader

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var (
	ErrLeaseNotFound = errors.New("leader lease not found")
	ErrLeaseConflict = errors.New("leader lease changed concurrently")
	ErrNotLeader     = errors.New("instance is not the current leader")
)

type Lease struct {
	ResourceVersion string
	UID             string
	Holder          string
	Transitions     int64
	RenewedAt       time.Time
	Duration        time.Duration
}

func (l Lease) Token() string {
	if l.UID == "" || l.Transitions < 1 {
		return ""
	}
	return fmt.Sprintf("%s:%d", l.UID, l.Transitions)
}

func (l Lease) Expired(serverNow time.Time) bool {
	return l.RenewedAt.IsZero() || l.Duration <= 0 || !serverNow.Before(l.RenewedAt.Add(l.Duration))
}

type Store interface {
	Get(context.Context) (Lease, time.Time, error)
	Create(context.Context, Lease) (Lease, time.Time, error)
	Update(context.Context, Lease, Lease) (Lease, time.Time, error)
}

type Config struct {
	Identity      string
	LeaseDuration time.Duration
	RetryPeriod   time.Duration
	Store         Store
}

type Snapshot struct {
	Leader    bool
	Holder    string
	Token     string
	ExpiresAt time.Time
}

type Elector struct {
	identity      string
	leaseDuration time.Duration
	retryPeriod   time.Duration
	store         Store

	mu       sync.RWMutex
	snapshot Snapshot
}

func New(cfg Config) (*Elector, error) {
	switch {
	case strings.TrimSpace(cfg.Identity) == "":
		return nil, errors.New("leader identity must not be blank")
	case cfg.LeaseDuration < 2*time.Second:
		return nil, errors.New("leader lease duration must be at least 2 seconds")
	case cfg.RetryPeriod <= 0 || cfg.RetryPeriod >= cfg.LeaseDuration:
		return nil, errors.New("leader retry period must be positive and shorter than lease duration")
	case cfg.Store == nil:
		return nil, errors.New("leader lease store is required")
	}
	return &Elector{
		identity:      strings.TrimSpace(cfg.Identity),
		leaseDuration: cfg.LeaseDuration,
		retryPeriod:   cfg.RetryPeriod,
		store:         cfg.Store,
	}, nil
}

func (e *Elector) Snapshot() Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.snapshot
}

func (e *Elector) Run(ctx context.Context) {
	e.step(ctx)
	ticker := time.NewTicker(e.retryPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			e.clear()
			return
		case <-ticker.C:
			e.step(ctx)
		}
	}
}

func (e *Elector) Confirm(ctx context.Context) (string, error) {
	current := e.Snapshot()
	if !current.Leader || current.Token == "" {
		return "", ErrNotLeader
	}

	lease, serverNow, err := e.store.Get(ctx)
	if err != nil {
		e.clear()
		return "", fmt.Errorf("confirm leader lease: %w", err)
	}
	if lease.Holder != e.identity || lease.Token() != current.Token || lease.Expired(serverNow) {
		e.observe(lease, serverNow, false)
		return "", ErrNotLeader
	}
	e.observe(lease, serverNow, true)
	return current.Token, nil
}

func (e *Elector) step(ctx context.Context) {
	current, serverNow, err := e.store.Get(ctx)
	if errors.Is(err, ErrLeaseNotFound) {
		desired := Lease{
			Holder: e.identity, Transitions: 1,
			RenewedAt: serverNow, Duration: e.leaseDuration,
		}
		created, createdAt, createErr := e.store.Create(ctx, desired)
		if createErr != nil {
			e.clear()
			return
		}
		e.observe(created, createdAt, true)
		return
	}
	if err != nil {
		e.clear()
		return
	}

	if !current.Expired(serverNow) && current.Holder != e.identity {
		e.observe(current, serverNow, false)
		return
	}

	desired := current
	desired.Holder = e.identity
	desired.RenewedAt = serverNow
	desired.Duration = e.leaseDuration
	if current.Expired(serverNow) {
		desired.Transitions++
		if desired.Transitions < 1 {
			desired.Transitions = 1
		}
	}
	updated, updatedAt, updateErr := e.store.Update(ctx, current, desired)
	if updateErr != nil {
		e.clear()
		return
	}
	e.observe(updated, updatedAt, true)
}

func (e *Elector) observe(lease Lease, serverNow time.Time, leader bool) {
	token := lease.Token()
	leader = leader && lease.Holder == e.identity && token != "" && !lease.Expired(serverNow)
	e.mu.Lock()
	e.snapshot = Snapshot{
		Leader: leader, Holder: lease.Holder,
		Token: token, ExpiresAt: lease.RenewedAt.Add(lease.Duration),
	}
	e.mu.Unlock()
}

func (e *Elector) clear() {
	e.mu.Lock()
	e.snapshot = Snapshot{}
	e.mu.Unlock()
}
