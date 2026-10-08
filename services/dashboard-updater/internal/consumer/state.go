package consumer

import (
	"sort"
	"sync"
	"time"

	"dashboard-updater/internal/hazard"
)

type State struct {
	mu     sync.RWMutex
	events map[string]Entry
}

type Entry struct {
	HazardID   string          `json:"hazard_id"`
	HazardType hazard.Type     `json:"hazard_type"`
	Severity   hazard.Severity `json:"severity"`
	AreaName   string          `json:"area_name"`
	OccurredAt time.Time       `json:"occurred_at"`
	ReceivedAt time.Time       `json:"received_at"`
}

type Snapshot struct {
	Total      int                     `json:"total"`
	BySeverity map[hazard.Severity]int `json:"by_severity"`
	ByType     map[hazard.Type]int     `json:"by_type"`
	Latest     []Entry                 `json:"latest"`
}

func NewState() *State { return &State{events: map[string]Entry{}} }

func (s *State) Apply(ev hazard.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events[ev.HazardID] = Entry{
		HazardID:   ev.HazardID,
		HazardType: ev.HazardType,
		Severity:   ev.Severity,
		AreaName:   ev.AreaName,
		OccurredAt: ev.OccurredAt,
		ReceivedAt: time.Now().UTC(),
	}
}

func (s *State) Snapshot(latest int) Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := Snapshot{
		Total:      len(s.events),
		BySeverity: map[hazard.Severity]int{},
		ByType:     map[hazard.Type]int{},
	}
	all := make([]Entry, 0, len(s.events))
	for _, e := range s.events {
		snap.BySeverity[e.Severity]++
		snap.ByType[e.HazardType]++
		all = append(all, e)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].OccurredAt.After(all[j].OccurredAt) })
	if len(all) > latest {
		all = all[:latest]
	}
	snap.Latest = all
	return snap
}
