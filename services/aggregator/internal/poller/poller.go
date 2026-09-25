package poller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"time"

	"aggregator/internal/bmkgclient"
	"aggregator/internal/broker"
	"aggregator/internal/hazard"
	"aggregator/internal/mapper"
	"aggregator/internal/pvmbgclient"
	"aggregator/internal/store"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PVMBGStatus struct {
	Available  bool       `json:"available"`
	StaleSince *time.Time `json:"stale_since,omitempty"`
}

type Poller struct {
	bmkg     *bmkgclient.Client
	pvmbg    *pvmbgclient.Client
	store    *store.HazardStore
	rabbitCh *amqp.Channel
	exchange string
	interval time.Duration
	log      *slog.Logger

	bmkgSince  string
	pvmbgSince string

	statusMu         sync.RWMutex
	pvmbgAvailable   bool
	pvmbgLastSuccess time.Time
}

func New(bmkg *bmkgclient.Client, pvmbg *pvmbgclient.Client, hazardStore *store.HazardStore, rabbitCh *amqp.Channel, exchange string, interval time.Duration, log *slog.Logger) *Poller {
	return &Poller{
		bmkg:           bmkg,
		pvmbg:          pvmbg,
		store:          hazardStore,
		rabbitCh:       rabbitCh,
		exchange:       exchange,
		interval:       interval,
		log:            log,
		pvmbgAvailable: true,
	}
}

func (p *Poller) PVMBGStatus() PVMBGStatus {
	p.statusMu.RLock()
	defer p.statusMu.RUnlock()

	status := PVMBGStatus{Available: p.pvmbgAvailable}
	if !p.pvmbgAvailable && !p.pvmbgLastSuccess.IsZero() {
		since := p.pvmbgLastSuccess
		status.StaleSince = &since
	}
	return status
}

func (p *Poller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	p.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tick(ctx)
		}
	}
}

func (p *Poller) tick(ctx context.Context) {
	done := make(chan struct{}, 2)

	go func() {
		p.pollBMKG(ctx)
		done <- struct{}{}
	}()
	go func() {
		p.pollPVMBG(ctx)
		done <- struct{}{}
	}()

	<-done
	<-done
}

func (p *Poller) pollBMKG(ctx context.Context) {
	correlationID := newCorrelationID()
	now := time.Now().UTC().Format(time.RFC3339)

	events, err := p.bmkg.SeismicEvents(ctx, p.bmkgSince, correlationID)
	if err != nil {
		p.log.Warn("bmkg poll failed", "err", err, "correlation_id", correlationID)
		return
	}

	warnings, err := p.bmkg.TsunamiWarnings(ctx, p.bmkgSince, correlationID)
	if err != nil {
		p.log.Warn("bmkg tsunami warnings poll failed", "err", err, "correlation_id", correlationID)
		warnings = nil
	}

	warningByEventID := map[string]map[string]any{}
	for _, w := range warnings {
		relatedID, _ := w["related_event_id"].(string)
		if relatedID != "" {
			warningByEventID[relatedID] = w
		}
	}

	for _, raw := range events {
		eventID, _ := raw["event_id"].(string)
		event, err := mapper.MapSeismicEvent(raw, warningByEventID[eventID])
		if err != nil {
			p.log.Error("failed to map seismic event", "err", err, "correlation_id", correlationID)
			continue
		}
		p.persist(event, correlationID)
	}

	p.bmkgSince = now
}

func (p *Poller) pollPVMBG(ctx context.Context) {
	correlationID := newCorrelationID()
	now := time.Now().UTC().Format(time.RFC3339)

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

	for _, raw := range reports {
		event, err := mapper.MapVolcanicReport(raw)
		if err != nil {
			p.log.Error("failed to map volcanic report", "err", err, "correlation_id", correlationID)
			continue
		}
		p.persist(event, correlationID)
	}

	p.pvmbgSince = now
}

func (p *Poller) markPVMBGAvailable() {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	p.pvmbgAvailable = true
	p.pvmbgLastSuccess = time.Now().UTC()
}

func (p *Poller) markPVMBGUnavailable() {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	if p.pvmbgAvailable && p.pvmbgLastSuccess.IsZero() {
		p.pvmbgLastSuccess = time.Now().UTC()
	}
	p.pvmbgAvailable = false
}

func (p *Poller) persist(event hazard.Event, correlationID string) {
	if err := p.store.SaveHazardEvent(event); err != nil {
		p.log.Error("failed to save hazard event", "err", err, "hazard_id", event.HazardID, "correlation_id", correlationID)
		return
	}
	if p.rabbitCh != nil {
		if err := broker.Publish(p.rabbitCh, p.exchange, event); err != nil {
			p.log.Error("failed to publish hazard event", "err", err, "hazard_id", event.HazardID, "correlation_id", correlationID)
			return
		}
	}
	p.log.Info("hazard event ingested", "hazard_id", event.HazardID, "hazard_type", event.HazardType, "severity", event.Severity, "correlation_id", correlationID)
}

func newCorrelationID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return time.Now().UTC().Format("20060102T150405.000000000")
	}
	return hex.EncodeToString(value[:])
}
