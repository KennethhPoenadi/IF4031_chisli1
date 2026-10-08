package pvmbg

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type VolcanicReport struct {
	ReportID         string    `json:"report_id"`
	VolcanoID        string    `json:"volcano_id"`
	AlertLevel       string    `json:"alert_level"`
	EruptionCount24H int       `json:"eruption_count_24h"`
	AshColumnHeightM float64   `json:"ash_column_height_m"`
	ReportedAt       time.Time `json:"reported_at"`
	ConfidenceLevel  *float64  `json:"confidence_level,omitempty"`
}

type Store struct {
	mu            sync.RWMutex
	reports       []VolcanicReport
	nextID        int
	schemaEnabled bool
	outage        bool
	rng           *rand.Rand
	now           func() time.Time
}

func NewStore(seedCount int, randomSeed int64, now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	store := &Store{
		reports: make([]VolcanicReport, 0, seedCount+16),
		rng:     rand.New(rand.NewSource(randomSeed)),
		now:     now,
	}
	base := now().UTC().Add(-time.Duration(seedCount) * time.Minute)
	for i := 0; i < seedCount; i++ {
		store.generateLocked(base.Add(time.Duration(i) * time.Minute))
	}
	return store
}

func (s *Store) ReportsSince(since *time.Time) []VolcanicReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]VolcanicReport, 0, len(s.reports))
	for _, report := range s.reports {
		if since == nil || report.ReportedAt.After(*since) {
			result = append(result, report)
		}
	}
	return result
}

func (s *Store) SetSchemaEnabled(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wasEnabled := s.schemaEnabled
	s.schemaEnabled = enabled
	if enabled && !wasEnabled {
		s.generateLocked(s.now().UTC())
	}
}

func (s *Store) SchemaEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.schemaEnabled
}

func (s *Store) SetOutage(enabled bool) {
	s.mu.Lock()
	s.outage = enabled
	s.mu.Unlock()
}

func (s *Store) Outage() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.outage
}

func (s *Store) GenerateNow() VolcanicReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generateLocked(s.now().UTC())
}

func (s *Store) RandomDelay(minimum, maximum time.Duration) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if maximum <= minimum {
		return minimum
	}
	return minimum + time.Duration(s.rng.Int63n(int64(maximum-minimum)+1))
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

func (s *Store) generateLocked(reportedAt time.Time) VolcanicReport {
	s.nextID++
	report := VolcanicReport{
		ReportID:         fmt.Sprintf("pvmbg-%06d", s.nextID),
		VolcanoID:        []string{"G-001", "G-002", "G-003", "G-004"}[(s.nextID-1)%4],
		AlertLevel:       []string{"Normal", "Waspada", "Siaga", "Awas"}[(s.nextID-1)%4],
		EruptionCount24H: s.rng.Intn(12),
		AshColumnHeightM: round1(100 + s.rng.Float64()*4900),
		ReportedAt:       reportedAt.UTC(),
	}
	if s.schemaEnabled {
		confidence := round2(0.7 + s.rng.Float64()*0.3)
		report.ConfidenceLevel = &confidence
	}
	s.reports = append(s.reports, report)
	return report
}

func round1(value float64) float64 { return float64(int(value*10+0.5)) / 10 }
func round2(value float64) float64 { return float64(int(value*100+0.5)) / 100 }
