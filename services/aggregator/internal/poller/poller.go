package poller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/mapper"
	"aggregator/internal/pantaiclient"
	"aggregator/internal/pvmbgclient"
	"aggregator/internal/resilience"
	"telemetry"
)

type Publisher interface {
	Publish(ctx context.Context, event hazard.Event, correlationID string) error
}

type SeismicSource interface {
	SeismicEvents(ctx context.Context, since string, correlationID string) ([]map[string]any, error)
	TsunamiWarnings(ctx context.Context, since string, correlationID string) ([]map[string]any, error)
}

type VolcanicSource interface {
	VolcanicReports(ctx context.Context, since string, correlationID string) ([]map[string]any, error)
}

type EventStore interface {
	SaveHazardEvent(event hazard.Event) error
}

type CoastalSource interface {
	Fetch(ctx context.Context, cursor string) (pantaiclient.Page, error)
	Stations(ctx context.Context) (pantaiclient.StationPage, error)
}

type CoastalRejection struct {
	Observation   map[string]any
	Reason        string
	CorrelationID string
	RejectedAt    time.Time
}

type CoastalStateStore interface {
	LoadCursor(ctx context.Context) (string, error)
	SaveCursor(ctx context.Context, cursor string) error
	Quarantine(ctx context.Context, rejection CoastalRejection) error
}

type CoastalConfig struct {
	Source          CoastalSource
	StateStore      CoastalStateStore
	Breaker         *resilience.Breaker
	PollInterval    time.Duration
	StaleAfter      time.Duration
	StationCacheTTL time.Duration
	Now             func() time.Time
}

const cursorFormat = time.RFC3339Nano

const (
	SourceHealthy  = "HEALTHY"
	SourceDegraded = "DEGRADED"
)

type PVMBGStatus struct {
	SourceStatus  string     `json:"source_status"`
	LastSuccessAt *time.Time `json:"last_success_at,omitempty"`
	StaleSince    *time.Time `json:"stale_since,omitempty"`
	Stale         bool       `json:"stale"`
}

type Poller struct {
	bmkg                   SeismicSource
	pvmbg                  VolcanicSource
	store                  EventStore
	publisher              Publisher
	interval               time.Duration
	staleAfter             time.Duration
	log                    *slog.Logger
	coastal                CoastalSource
	coastalState           CoastalStateStore
	coastalBreaker         *resilience.Breaker
	coastalPollInterval    time.Duration
	coastalStaleAfter      time.Duration
	coastalStationCacheTTL time.Duration
	coastalNow             func() time.Time
	coastalStations        map[string]map[string]any
	coastalStationsAt      time.Time

	bmkgSince  string
	pvmbgSince string

	statusMu      sync.RWMutex
	sourceStatus  string
	lastSuccessAt time.Time
	staleSince    time.Time
}

func (p *Poller) ConfigureCoastal(cfg CoastalConfig) error {
	if cfg.Source == nil {
		return errors.New("coastal source is required")
	}
	if cfg.StateStore == nil {
		return errors.New("coastal state store is required")
	}
	if cfg.Breaker == nil {
		return errors.New("coastal circuit breaker is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = p.interval
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = 30 * time.Second
	}
	if cfg.StationCacheTTL <= 0 {
		cfg.StationCacheTTL = 5 * time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	p.coastal = cfg.Source
	p.coastalState = cfg.StateStore
	p.coastalBreaker = cfg.Breaker
	p.coastalPollInterval = cfg.PollInterval
	p.coastalStaleAfter = cfg.StaleAfter
	p.coastalStationCacheTTL = cfg.StationCacheTTL
	p.coastalNow = cfg.Now
	return nil
}

func New(bmkg SeismicSource, pvmbg VolcanicSource, hazardStore EventStore, publisher Publisher, interval time.Duration, staleAfter time.Duration, log *slog.Logger) *Poller {
	return &Poller{
		bmkg:         bmkg,
		pvmbg:        pvmbg,
		store:        hazardStore,
		publisher:    publisher,
		interval:     interval,
		staleAfter:   staleAfter,
		log:          log,
		sourceStatus: SourceHealthy,
	}
}

func (p *Poller) PVMBGStatus() PVMBGStatus {
	p.statusMu.RLock()
	defer p.statusMu.RUnlock()

	status := PVMBGStatus{SourceStatus: p.sourceStatus}
	if !p.lastSuccessAt.IsZero() {
		t := p.lastSuccessAt
		status.LastSuccessAt = &t
	}
	if !p.staleSince.IsZero() {
		t := p.staleSince
		status.StaleSince = &t
		status.Stale = time.Since(p.staleSince) >= p.staleAfter
	}
	return status
}

func (p *Poller) Run(ctx context.Context) {
	var workers sync.WaitGroup
	workers.Add(2)
	if p.coastal != nil {
		workers.Add(1)
	}

	go func() {
		defer workers.Done()
		p.runBMKGWorker(ctx)
	}()

	go func() {
		defer workers.Done()
		p.runPVMBGWorker(ctx)
	}()

	if p.coastal != nil {
		go func() {
			defer workers.Done()
			p.runCoastalWorker(ctx)
		}()
	}

	workers.Wait()
}

func (p *Poller) runBMKGWorker(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	p.pollBMKG(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pollBMKG(ctx)
		}
	}
}

func (p *Poller) runPVMBGWorker(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	p.pollPVMBG(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pollPVMBG(ctx)
		}
	}
}

func (p *Poller) pollBMKG(ctx context.Context) {
	correlationID := newCorrelationID()
	now := time.Now().UTC().Format(cursorFormat)

	events, err := p.bmkg.SeismicEvents(ctx, p.bmkgSince, correlationID)
	if err != nil {
		p.log.Warn("bmkg poll failed", "err", err, "correlation_id", correlationID)
		return
	}

	warnings, err := p.bmkg.TsunamiWarnings(ctx, p.bmkgSince, correlationID)
	if err != nil {
		p.log.Warn("bmkg tsunami warnings poll failed, cycle skipped to avoid wrong severity", "err", err, "correlation_id", correlationID)
		return
	}

	warningByEventID := map[string]map[string]any{}
	for _, w := range warnings {
		relatedID, _ := w["related_event_id"].(string)
		if relatedID != "" {
			warningByEventID[relatedID] = w
		}
	}

	inBatch := make(map[string]bool, len(events))
	for _, raw := range events {
		id, _ := raw["event_id"].(string)
		inBatch[id] = true
	}
	var orphan bool
	for relatedID := range warningByEventID {
		if !inBatch[relatedID] {
			orphan = true
			break
		}
	}
	if orphan {
		all, err := p.bmkg.SeismicEvents(ctx, "", correlationID)
		if err != nil {
			p.log.Warn("bmkg refetch for late tsunami warning failed", "err", err, "correlation_id", correlationID)
			return
		}
		for _, raw := range all {
			id, _ := raw["event_id"].(string)
			if _, hasWarning := warningByEventID[id]; hasWarning && !inBatch[id] {
				events = append(events, raw)
				inBatch[id] = true
			}
		}
	}

	failed := false
	for _, raw := range events {
		eventID, _ := raw["event_id"].(string)
		event, err := mapper.MapSeismicEvent(raw, warningByEventID[eventID])
		if err != nil {
			p.log.Error("failed to map seismic event", "err", err, "correlation_id", correlationID)
			continue
		}
		if err := p.persist(ctx, event, correlationID); err != nil {
			failed = true
			break
		}
	}

	if failed {
		return
	}
	p.bmkgSince = now
}

func (p *Poller) pollPVMBG(ctx context.Context) {
	correlationID := newCorrelationID()
	now := time.Now().UTC().Format(cursorFormat)

	reports, err := p.pvmbg.VolcanicReports(ctx, p.pvmbgSince, correlationID)
	if err != nil {
		p.markPVMBGUnavailable()
		if errors.Is(err, pvmbgclient.ErrOutage) {
			p.log.Warn("pvmbg is in outage, skipping this cycle, seismic data unaffected", "correlation_id", correlationID)
		} else {
			p.log.Warn("pvmbg poll failed", "err", err, "correlation_id", correlationID)
		}
		return
	}
	p.markPVMBGAvailable()

	failed := false
	for _, raw := range reports {
		event, err := mapper.MapVolcanicReport(raw)
		if err != nil {
			p.log.Error("failed to map volcanic report", "err", err, "correlation_id", correlationID)
			continue
		}
		if err := p.persist(ctx, event, correlationID); err != nil {
			failed = true
			break
		}
	}
	if failed {
		return
	}

	p.pvmbgSince = now
}

func (p *Poller) markPVMBGAvailable() {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	p.sourceStatus = SourceHealthy
	p.lastSuccessAt = time.Now().UTC()
	p.staleSince = time.Time{}
}

func (p *Poller) markPVMBGUnavailable() {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	p.sourceStatus = SourceDegraded
	if p.staleSince.IsZero() {
		p.staleSince = time.Now().UTC()
	}
}

var (
	ingested = telemetry.Counter("hazard_events_ingested", "HazardEvent yang berhasil disimpan dan dipublikasikan")
	failed   = telemetry.Counter("hazard_events_failed", "HazardEvent yang gagal diproses, per tahap")
)

type contextSaver interface {
	Save(ctx context.Context, event hazard.Event) error
}

func (p *Poller) persist(ctx context.Context, event hazard.Event, correlationID string) (err error) {
	ctx, endSpan := telemetry.StartSpan(ctx, "ingest hazard event",
		"hazard.id", event.HazardID, "hazard.type", string(event.HazardType),
		"hazard.source", string(event.Source), "correlation.id", correlationID)
	defer func() { endSpan(err) }()
	attrs := []any{"source", string(event.Source), "type", string(event.HazardType)}

	saveStart := time.Now()
	var saveErr error
	if cs, ok := p.store.(contextSaver); ok {
		saveErr = cs.Save(ctx, event)
	} else {
		saveErr = p.store.SaveHazardEvent(event)
	}
	p.logCall("mongodb", "save_hazard_event", saveStart, saveErr, correlationID, event.HazardID)
	if saveErr != nil {
		failed(ctx, 1, append(attrs, "stage", "store")...)
		p.log.Error("failed to save hazard event", append([]any{"err", saveErr, "hazard_id", event.HazardID, "correlation_id", correlationID}, telemetry.LogAttrs(ctx)...)...)
		return saveErr
	}
	if p.publisher != nil {
		publishStart := time.Now()
		publishErr := p.publisher.Publish(ctx, event, correlationID)
		p.logCall("rabbitmq", "publish", publishStart, publishErr, correlationID, event.HazardID)
		if publishErr != nil {
			failed(ctx, 1, append(attrs, "stage", "publish")...)
			p.log.Error("failed to publish hazard event", append([]any{"err", publishErr, "hazard_id", event.HazardID, "correlation_id", correlationID}, telemetry.LogAttrs(ctx)...)...)
			return publishErr
		}
	}
	ingested(ctx, 1, append(attrs, "severity", string(event.Severity))...)
	p.log.Info("hazard event ingested", append([]any{"hazard_id", event.HazardID, "hazard_type", event.HazardType, "severity", event.Severity, "correlation_id", correlationID}, telemetry.LogAttrs(ctx)...)...)
	return nil
}

// logCall records the latency of one outgoing call to a datastore or the broker.
func (p *Poller) logCall(target, operation string, start time.Time, err error, correlationID, hazardID string) {
	attrs := []any{"target", target, "operation", operation, "duration_ms", time.Since(start).Milliseconds(),
		"success", err == nil, "hazard_id", hazardID, "correlation_id", correlationID}
	if err != nil {
		p.log.Warn(target+" call failed", append(attrs, "err", err)...)
		return
	}
	p.log.Info(target+" call", attrs...)
}

func newCorrelationID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return time.Now().UTC().Format("20060102T150405.000000000")
	}
	return hex.EncodeToString(value[:])
}
