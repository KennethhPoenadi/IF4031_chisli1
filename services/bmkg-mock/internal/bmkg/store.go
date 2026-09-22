package bmkg

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type SeismicEvent struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKM          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
}

type TsunamiWarning struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"`
	AffectedZones    []string  `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
}

type tsunamiWarningRecord struct {
	warning   TsunamiWarning
	createdAt time.Time
}

type Store struct {
	mu       sync.RWMutex
	events   []SeismicEvent
	warnings []tsunamiWarningRecord
	nextID   int
	rng      *rand.Rand
	now      func() time.Time
}

func NewStore(seedCount int, randomSeed int64, now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	store := &Store{
		events:   make([]SeismicEvent, 0, seedCount+16),
		warnings: make([]tsunamiWarningRecord, 0, seedCount/4+4),
		rng:      rand.New(rand.NewSource(randomSeed)),
		now:      now,
	}
	base := now().UTC().Add(-time.Duration(seedCount) * time.Minute)
	for i := 0; i < seedCount; i++ {
		store.generateLocked(base.Add(time.Duration(i) * time.Minute))
	}
	return store
}

func (s *Store) EventsSince(since *time.Time) []SeismicEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]SeismicEvent, 0, len(s.events))
	for _, event := range s.events {
		if since == nil || event.OccurredAt.After(*since) {
			result = append(result, event)
		}
	}
	return result
}

func (s *Store) WarningsSince(since *time.Time) []TsunamiWarning {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]TsunamiWarning, 0, len(s.warnings))
	for _, record := range s.warnings {
		if since == nil || record.createdAt.After(*since) {
			result = append(result, record.warning)
		}
	}
	return result
}

func (s *Store) GenerateNow() SeismicEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generateLocked(s.now().UTC())
}

func (s *Store) RunGenerator(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.GenerateNow()
		}
	}
}

func (s *Store) generateLocked(occurredAt time.Time) SeismicEvent {
	s.nextID++
	potentialTsunami := s.nextID%4 == 0
	event := SeismicEvent{
		EventID:          fmt.Sprintf("bmkg-%06d", s.nextID),
		Magnitude:        round1(4.0 + s.rng.Float64()*3.5),
		DepthKM:          round1(5 + s.rng.Float64()*145),
		EpicenterLat:     round4(-11 + s.rng.Float64()*17),
		EpicenterLon:     round4(95 + s.rng.Float64()*46),
		RegionName:       []string{"Selat Sunda", "Laut Banda", "Pesisir Selatan Jawa", "Sulawesi Utara"}[(s.nextID-1)%4],
		OccurredAt:       occurredAt.UTC(),
		PotentialTsunami: potentialTsunami,
	}
	s.events = append(s.events, event)
	if potentialTsunami {
		levels := []string{"Waspada", "Siaga", "Awas"}
		s.warnings = append(s.warnings, tsunamiWarningRecord{
			warning: TsunamiWarning{
				WarningID:        fmt.Sprintf("tsunami-%06d", s.nextID),
				RelatedEventID:   event.EventID,
				ThreatLevel:      levels[(s.nextID/4-1)%len(levels)],
				AffectedZones:    []string{event.RegionName},
				EstimatedArrival: event.OccurredAt.Add(45 * time.Minute),
			},
			createdAt: event.OccurredAt,
		})
	}
	return event
}

func round1(value float64) float64 { return float64(int(value*10+0.5)) / 10 }
func round4(value float64) float64 { return float64(int(value*10000+0.5)) / 10000 }
