package poller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"aggregator/internal/mapper"
	"aggregator/internal/resilience"
)

const maxCoastalPagesPerCycle = 100

func (p *Poller) runCoastalWorker(ctx context.Context) {
	ticker := time.NewTicker(p.coastalPollInterval)
	defer ticker.Stop()

	cursor := ""
	loaded := false
	for {
		if !loaded {
			var err error
			cursor, err = p.coastalState.LoadCursor(ctx)
			if err != nil {
				p.log.Warn("failed to load Sensor Pantai cursor", "err", err)
			} else {
				loaded = true
			}
		}
		if loaded {
			cursor = p.pollCoastal(ctx, cursor)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Poller) pollCoastal(ctx context.Context, cursor string) string {
	var allowed bool
	if ctx, allowed = p.beginPoll(ctx); !allowed {
		return cursor
	}
	permit, err := p.coastalBreaker.Allow()
	if err != nil {
		if errors.Is(err, resilience.ErrOpen) {
			p.log.Warn("Sensor Pantai circuit breaker open", "state", p.coastalBreaker.State().String())
		} else {
			p.log.Warn("Sensor Pantai circuit breaker rejected request", "err", err)
		}
		return cursor
	}

	next, dependencyFailed := p.pollCoastalAllowed(ctx, cursor)
	if dependencyFailed {
		permit.Failure()
	} else {
		permit.Success()
	}
	return next
}

func (p *Poller) pollCoastalAllowed(ctx context.Context, cursor string) (next string, dependencyFailed bool) {
	correlationID := newCorrelationID()
	stations, stationErr := p.coastalStationCatalog(ctx)
	if stationErr != nil && stations == nil {
		p.log.Warn("Sensor Pantai station fetch failed", "err", stationErr, "correlation_id", correlationID)
		return cursor, true
	}
	dependencyFailed = stationErr != nil
	if stationErr != nil {
		p.log.Warn("Sensor Pantai station refresh failed; using cached catalog", "err", stationErr,
			"correlation_id", correlationID)
	}

	current := cursor
	for pageNo := 0; pageNo < maxCoastalPagesPerCycle; pageNo++ {
		if err := p.requirePollLeadership(ctx); err != nil {
			return current, dependencyFailed
		}
		page, err := p.coastal.Fetch(ctx, current)
		if err != nil {
			p.log.Warn("Sensor Pantai observation fetch failed", "err", err, "correlation_id", correlationID)
			return current, true
		}
		if err := p.rejectStaleCoastalPage(page.GeneratedAt); err != nil {
			p.log.Warn("Sensor Pantai returned stale data", "err", err, "correlation_id", correlationID)
			return current, true
		}
		if page.NextCursor != "" && page.NextCursor == current {
			p.log.Warn("Sensor Pantai returned a repeated cursor", "cursor", current, "correlation_id", correlationID)
			return current, true
		}

		sort.SliceStable(page.Observations, func(i, j int) bool {
			left, _ := page.Observations[i]["critical"].(bool)
			right, _ := page.Observations[j]["critical"].(bool)
			return left && !right
		})
		for _, raw := range page.Observations {
			stationID, _ := raw["station_id"].(string)
			event, mapErr := mapper.MapCoastalEvent(raw, stations[stationID])
			if mapErr != nil {
				rejection := CoastalRejection{
					Observation: raw, Reason: mapErr.Error(), CorrelationID: correlationID,
					RejectedAt: p.coastalNow().UTC(),
				}
				if err := p.requirePollLeadership(ctx); err != nil {
					return current, dependencyFailed
				}
				if err := p.coastalState.Quarantine(ctx, rejection); err != nil {
					p.log.Error("failed to quarantine Sensor Pantai observation", "err", err,
						"reason", mapErr, "correlation_id", correlationID)
					return current, dependencyFailed
				}
				continue
			}
			if err := p.persist(ctx, event, correlationID); err != nil {
				return current, dependencyFailed
			}
		}

		if page.NextCursor == "" {
			return current, dependencyFailed
		}
		if err := p.requirePollLeadership(ctx); err != nil {
			return current, dependencyFailed
		}
		if err := p.coastalState.SaveCursor(ctx, page.NextCursor); err != nil {
			p.log.Error("failed to save Sensor Pantai cursor", "err", err, "cursor", page.NextCursor,
				"correlation_id", correlationID)
			return current, dependencyFailed
		}
		current = page.NextCursor
	}

	p.log.Warn("Sensor Pantai exceeded page safety limit", "limit", maxCoastalPagesPerCycle,
		"correlation_id", correlationID)
	return current, true
}

func (p *Poller) coastalStationCatalog(ctx context.Context) (map[string]map[string]any, error) {
	now := p.coastalNow()
	if p.coastalStations != nil && now.Sub(p.coastalStationsAt) < p.coastalStationCacheTTL {
		return p.coastalStations, nil
	}

	if err := p.requirePollLeadership(ctx); err != nil {
		return nil, err
	}
	page, err := p.coastal.Stations(ctx)
	if err != nil {
		if p.coastalStations != nil {
			return p.coastalStations, err
		}
		return nil, err
	}
	stations := make(map[string]map[string]any, len(page.Stations))
	for _, station := range page.Stations {
		id, _ := station["station_id"].(string)
		if id != "" {
			stations[id] = station
		}
	}
	p.coastalStations = stations
	p.coastalStationsAt = now
	return stations, nil
}

// rejectStaleCoastalPage menolak data basi berdasarkan kemajuan jam sensor itu sendiri, bukan jam lokal.
// Sensor dapat berjalan pada jam virtual (bahkan dipercepat) yang tidak sama dengan jam Aggregator, sehingga
// membandingkan generated_at dengan jam lokal salah menilai semua data sebagai basi. Yang menandakan data
// basi adalah sensor yang tetap menjawab (mengaku sehat) tetapi jamnya berhenti maju lebih lama dari
// coastalStaleAfter. Jam yang mundur (sensor dimulai ulang) diterima sebagai garis waktu baru.
func (p *Poller) rejectStaleCoastalPage(generatedAt string) error {
	timestamp, err := time.Parse(time.RFC3339, generatedAt)
	if err != nil {
		return fmt.Errorf("generated_at is invalid: %w", err)
	}
	now := p.coastalNow()
	switch {
	case p.coastalLastGenerated.IsZero() || timestamp.After(p.coastalLastGenerated):
		p.coastalLastGenerated, p.coastalLastAdvancedAt = timestamp, now
		return nil
	case timestamp.Before(p.coastalLastGenerated):
		p.log.Warn("jam Sensor Pantai mundur, diterima sebagai garis waktu baru",
			"previous", p.coastalLastGenerated, "generated_at", timestamp)
		p.coastalLastGenerated, p.coastalLastAdvancedAt = timestamp, now
		return nil
	}
	if frozen := now.Sub(p.coastalLastAdvancedAt); frozen > p.coastalStaleAfter {
		return fmt.Errorf("data is stale: sensor clock has not advanced for %s (generated_at %s)",
			frozen.Round(time.Second), generatedAt)
	}
	return nil
}
