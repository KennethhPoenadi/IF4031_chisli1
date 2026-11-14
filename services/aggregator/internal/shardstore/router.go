package shardstore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"aggregator/internal/hazard"
	"telemetry"
)

var degradedWrites = telemetry.Counter("store_writes_degraded", "penulisan yang tersimpan kurang dari jumlah salinan yang seharusnya")

func (r *Router) RegisterMetrics() {
	telemetry.GaugeFunc("store_unit_up", "1 bila unit penyimpanan terjangkau, 0 bila tidak", func() []telemetry.Sample {
		down := r.downSet()
		names := r.UnitNames()
		out := make([]telemetry.Sample, 0, len(names))
		for _, n := range names {
			v := 1.0
			if down[n] {
				v = 0
			}
			out = append(out, telemetry.Sample{Value: v, Attrs: []any{"unit", n}})
		}
		return out
	})
	telemetry.GaugeFunc("store_rebalancing", "1 bila penempatan data sedang disamakan (perbaikan tertunda)", func() []telemetry.Sample {
		v := 0.0
		if r.repairNeeded.Load() {
			v = 1
		}
		return []telemetry.Sample{{Value: v}}
	})
	telemetry.GaugeFunc("store_replication_factor", "jumlah salinan tiap event", func() []telemetry.Sample {
		return []telemetry.Sample{{Value: float64(r.rf)}}
	})
}

type Config struct {
	ReplicationFactor int
	VNodes            int
	UnitTimeout       time.Duration
	RepairTimeout     time.Duration
	PingInterval      time.Duration
	RepairInterval    time.Duration
	Nodes             NodeResolver
	Log               *slog.Logger
}

func (c *Config) defaults() {
	if c.ReplicationFactor < 1 {
		c.ReplicationFactor = 2
	}
	if c.VNodes < 1 {
		c.VNodes = 128
	}
	if c.UnitTimeout <= 0 {
		c.UnitTimeout = 2 * time.Second
	}
	if c.RepairTimeout <= 0 {
		c.RepairTimeout = 5 * time.Minute
	}
	if c.PingInterval <= 0 {
		c.PingInterval = 2 * time.Second
	}
	if c.RepairInterval <= 0 {
		c.RepairInterval = 30 * time.Second
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

type Router struct {
	cfg   Config
	units map[string]Unit
	ring  *Ring
	rf    int

	mu   sync.Mutex
	down map[string]bool

	repairNeeded atomic.Bool
	repairMu     sync.Mutex
	coastalMu    sync.Mutex
}

func New(units []Unit, cfg Config) (*Router, error) {
	if len(units) == 0 {
		return nil, fmt.Errorf("shardstore: minimal satu unit penyimpanan")
	}
	cfg.defaults()
	r := &Router{cfg: cfg, units: make(map[string]Unit, len(units)), down: map[string]bool{}}
	names := make([]string, 0, len(units))
	for _, u := range units {
		if _, dup := r.units[u.Name()]; dup {
			return nil, fmt.Errorf("shardstore: nama unit ganda %q", u.Name())
		}
		r.units[u.Name()] = u
		names = append(names, u.Name())
	}
	r.ring = NewRing(names, cfg.VNodes)
	r.rf = min(cfg.ReplicationFactor, len(units))
	return r, nil
}

func (r *Router) ReplicationFactor() int { return r.rf }

func (r *Router) Owners(hazardID string) []string { return r.ring.Owners(hazardID, r.rf) }

func (r *Router) UnitNames() []string { return r.ring.Units() }

func (r *Router) isDown(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.down[name]
}

func (r *Router) downSet() map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]bool, len(r.down))
	for k, v := range r.down {
		if v {
			out[k] = true
		}
	}
	return out
}

func (r *Router) markDown(name string, err error) {
	r.mu.Lock()
	was := r.down[name]
	r.down[name] = true
	r.mu.Unlock()
	if !was {
		r.cfg.Log.Warn("unit penyimpanan ditandai tidak terjangkau", "unit", name, "err", err)
	}
}

func (r *Router) markUp(name string) bool {
	r.mu.Lock()
	was := r.down[name]
	delete(r.down, name)
	r.mu.Unlock()
	return was
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (r *Router) each(ctx context.Context, op string, timeout time.Duration, fn func(ctx context.Context, u Unit) error) []string {
	failed := map[string]bool{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for name, u := range r.units {
		if r.isDown(name) {
			failed[name] = true
			continue
		}
		wg.Add(1)
		go func(name string, u Unit) {
			defer wg.Done()
			uctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			uctx, endSpan := telemetry.StartSpan(uctx, op, "db.system", "mongodb", "store.unit", name)
			err := fn(uctx, u)
			endSpan(err)
			if err != nil {
				r.markDown(name, err)
				mu.Lock()
				failed[name] = true
				mu.Unlock()
			}
		}(name, u)
	}
	wg.Wait()
	return sortedKeys(failed)
}

func (r *Router) SaveHazardEvent(ev hazard.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.UnitTimeout*2)
	defer cancel()
	return r.Save(ctx, ev)
}

func (r *Router) Save(ctx context.Context, ev hazard.Event) error {
	owners := r.Owners(ev.HazardID)

	var wg sync.WaitGroup
	var okCount atomic.Int32
	for _, name := range owners {
		if r.isDown(name) {
			continue
		}
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
			defer cancel()
			sctx, endSpan := telemetry.StartSpan(uctx, "store.upsert", "db.system", "mongodb", "store.unit", name, "hazard.id", ev.HazardID)
			err := r.units[name].Upsert(sctx, ev, owners)
			endSpan(err)
			if err != nil {
				r.markDown(name, err)
				return
			}
			okCount.Add(1)
		}(name)
	}
	wg.Wait()

	if okCount.Load() == 0 {
		for _, name := range r.ring.Owners(ev.HazardID, len(r.units)) {
			if contains(owners, name) || r.isDown(name) {
				continue
			}
			uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
			err := r.units[name].Upsert(uctx, ev, owners)
			cancel()
			if err != nil {
				r.markDown(name, err)
				continue
			}
			degradedWrites(ctx, 1)
			r.repairNeeded.Store(true)
			r.cfg.Log.Warn("pemilik tidak terjangkau, event dititipkan sementara", "hazard_id", ev.HazardID, "unit", name)
			return nil
		}
		return fmt.Errorf("%w: pemilik %v", ErrUnavailable, owners)
	}
	if int(okCount.Load()) < len(owners) {
		degradedWrites(ctx, 1)
		r.repairNeeded.Store(true)
		r.cfg.Log.Warn("event hanya tersimpan sebagian, menunggu perbaikan", "hazard_id", ev.HazardID,
			"tersimpan", okCount.Load(), "seharusnya", len(owners))
	}
	return nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

type cursorToken struct {
	T  string `json:"t"`
	ID string `json:"id"`
}

func encodeCursor(p Position) string {
	b, _ := json.Marshal(cursorToken{T: p.IngestedAt.UTC().Format(time.RFC3339Nano), ID: p.HazardID})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (*Position, error) {
	if s == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, ErrBadCursor
	}
	var tok cursorToken
	if err := json.Unmarshal(raw, &tok); err != nil || tok.ID == "" {
		return nil, ErrBadCursor
	}
	t, err := time.Parse(time.RFC3339Nano, tok.T)
	if err != nil {
		return nil, ErrBadCursor
	}
	return &Position{IngestedAt: t, HazardID: tok.ID}, nil
}

func less(a, b hazard.Event, order Order) bool {
	if !a.IngestedAt.Equal(b.IngestedAt) {
		if order == NewestFirst {
			return a.IngestedAt.After(b.IngestedAt)
		}
		return a.IngestedAt.Before(b.IngestedAt)
	}
	if order == NewestFirst {
		return a.HazardID > b.HazardID
	}
	return a.HazardID < b.HazardID
}

func (r *Router) ListEvents(ctx context.Context, q ListQuery) (Page, error) {
	if q.Limit < 1 {
		q.Limit = 50
	}
	after, err := decodeCursor(q.Cursor)
	if err != nil {
		return Page{}, err
	}

	var mu sync.Mutex
	merged := map[string]hazard.Event{}
	failed := r.each(ctx, "store.query", r.cfg.UnitTimeout, func(uctx context.Context, u Unit) error {
		events, err := u.Query(uctx, UnitQuery{Filter: q.Filter, After: after, Limit: q.Limit + 1, Order: q.Order})
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for _, ev := range events {
			if cur, ok := merged[ev.HazardID]; !ok || ev.IngestedAt.After(cur.IngestedAt) {
				merged[ev.HazardID] = ev
			}
		}
		return nil
	})
	if len(failed) == len(r.units) {
		return Page{}, ErrUnavailable
	}

	events := make([]hazard.Event, 0, len(merged))
	for _, ev := range merged {
		events = append(events, ev)
	}
	sort.Slice(events, func(i, j int) bool { return less(events[i], events[j], q.Order) })

	page := Page{Unavailable: failed}
	if len(events) > q.Limit {
		events = events[:q.Limit]
		last := events[len(events)-1]
		page.NextCursor = encodeCursor(Position{IngestedAt: last.IngestedAt, HazardID: last.HazardID})
	}
	page.Events = events
	page.Partial = !r.ring.Covered(r.downSet(), r.rf)
	return page, nil
}

type Placement struct {
	Node string `json:"node"`
	Unit string `json:"unit"`
	Role string `json:"role"`
}

type LocationResult struct {
	Found       bool
	Locations   []Placement
	Partial     bool
	Unavailable []string
}

func (r *Router) Location(ctx context.Context, hazardID string) (LocationResult, error) {
	var mu sync.Mutex
	var holders []string
	failed := r.each(ctx, "store.locate", r.cfg.UnitTimeout, func(uctx context.Context, u Unit) error {
		ev, err := u.Get(uctx, hazardID)
		if err != nil {
			return err
		}
		if ev != nil {
			mu.Lock()
			holders = append(holders, u.Name())
			mu.Unlock()
		}
		return nil
	})
	if len(failed) == len(r.units) {
		return LocationResult{}, ErrUnavailable
	}

	sort.Strings(holders)
	owners := r.Owners(hazardID)
	res := LocationResult{Found: len(holders) > 0, Partial: len(failed) > 0, Unavailable: failed}
	for _, h := range holders {
		role := "replica"
		if len(owners) > 0 && owners[0] == h {
			role = "primary"
		}
		node := ""
		if r.cfg.Nodes != nil {
			if n, err := r.cfg.Nodes.NodeOf(ctx, h); err == nil {
				node = n
			}
		}
		res.Locations = append(res.Locations, Placement{Node: node, Unit: h, Role: role})
	}
	sort.SliceStable(res.Locations, func(i, j int) bool {
		return res.Locations[i].Role == "primary" && res.Locations[j].Role != "primary"
	})
	return res, nil
}

type ChecksumResult struct {
	Source      string
	Count       int
	Checksum    string
	AsOf        time.Time
	Partial     bool
	Unavailable []string
}

func (r *Router) Checksum(ctx context.Context, source string) (ChecksumResult, error) {
	var mu sync.Mutex
	refs := map[string]string{}
	failed := r.each(ctx, "store.checksum", r.cfg.RepairTimeout, func(uctx context.Context, u Unit) error {
		local := map[string]string{}
		err := u.Keys(uctx, func(k KeyRecord) error {
			if source == "" || k.Source == source {
				local[k.HazardID] = k.SourceRefID
			}
			return nil
		})
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for id, ref := range local {
			refs[id] = ref
		}
		return nil
	})
	if len(failed) == len(r.units) {
		return ChecksumResult{}, ErrUnavailable
	}

	list := make([]string, 0, len(refs))
	for _, ref := range refs {
		list = append(list, ref)
	}
	sort.Strings(list)
	sum := sha256.Sum256([]byte(strings.Join(list, "\n")))
	return ChecksumResult{
		Source:      source,
		Count:       len(list),
		Checksum:    hex.EncodeToString(sum[:]),
		AsOf:        time.Now().UTC(),
		Partial:     !r.ring.Covered(r.downSet(), r.rf),
		Unavailable: failed,
	}, nil
}

type Status struct {
	Units       []string `json:"units"`
	Unavailable []string `json:"unavailable"`
	Replication int      `json:"replication_factor"`
}

func (r *Router) Status() Status {
	return Status{Units: r.UnitNames(), Unavailable: sortedKeys(r.downSet()), Replication: r.rf}
}

func (r *Router) Ping(ctx context.Context) error {
	if len(r.downSet()) >= len(r.units) {
		return ErrUnavailable
	}
	return nil
}

type Analytics struct {
	AggregateResult
	AsOf        time.Time
	Partial     bool
	Unavailable []string
	Rebalancing bool
}

func (r *Router) Analytics(ctx context.Context, q AggregateQuery) (Analytics, error) {
	var result AggregateResult
	var failed []string
	for attempt := 0; attempt < 2; attempt++ {
		down := sortedKeys(r.downSet())
		var mu sync.Mutex
		merged := NewAggregateResult()
		failed = r.each(ctx, "store.aggregate", r.cfg.RepairTimeout, func(uctx context.Context, u Unit) error {
			agg, err := u.Aggregate(uctx, q, down)
			if err != nil {
				return err
			}
			mu.Lock()
			merged.Merge(agg)
			mu.Unlock()
			return nil
		})
		result = merged
		if len(failed) == len(r.units) {
			return Analytics{}, ErrUnavailable
		}
		if len(failed) == len(down) {
			break
		}
	}
	return Analytics{
		AggregateResult: result,
		AsOf:            time.Now().UTC(),
		Partial:         !r.ring.Covered(r.downSet(), r.rf),
		Unavailable:     failed,
		Rebalancing:     r.repairNeeded.Load(),
	}, nil
}
