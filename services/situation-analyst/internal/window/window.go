package window

import (
	"sort"
	"sync"
	"time"
)

const maxAreasPerBucket = 500

type bucket struct {
	total      int
	byCategory map[string]int
	bySeverity map[string]int
	byArea     map[string]int
}

type Window struct {
	size    int
	now     func() time.Time
	mu      sync.Mutex
	buckets map[int64]*bucket
}

func New(size int, now func() time.Time) *Window {
	if now == nil {
		now = time.Now
	}
	return &Window{size: size, now: now, buckets: map[int64]*bucket{}}
}

func (w *Window) minute() int64 { return w.now().Unix() / 60 }

func (w *Window) prune(cur int64) {
	for m := range w.buckets {
		if m <= cur-int64(w.size) {
			delete(w.buckets, m)
		}
	}
}

func (w *Window) Add(category, severity, area string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	cur := w.minute()
	w.prune(cur)
	b, ok := w.buckets[cur]
	if !ok {
		b = &bucket{byCategory: map[string]int{}, bySeverity: map[string]int{}, byArea: map[string]int{}}
		w.buckets[cur] = b
	}
	b.total++
	b.byCategory[category]++
	b.bySeverity[severity]++
	if _, known := b.byArea[area]; known || len(b.byArea) < maxAreasPerBucket {
		b.byArea[area]++
	}
}

type AreaCount struct {
	Area  string `json:"area"`
	Count int    `json:"count"`
}

type Snapshot struct {
	WindowMinutes int            `json:"window_minutes"`
	Total         int            `json:"total"`
	ByCategory    map[string]int `json:"by_category"`
	BySeverity    map[string]int `json:"by_severity"`
	TopAreas      []AreaCount    `json:"top_areas"`
}

const TopAreasLimit = 5

func (w *Window) Snapshot(minutes int) Snapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	if minutes > w.size {
		minutes = w.size
	}
	cur := w.minute()
	w.prune(cur)

	snap := Snapshot{WindowMinutes: minutes, ByCategory: map[string]int{}, BySeverity: map[string]int{}}
	areas := map[string]int{}
	for m, b := range w.buckets {
		if m <= cur-int64(minutes) {
			continue
		}
		snap.Total += b.total
		for k, v := range b.byCategory {
			snap.ByCategory[k] += v
		}
		for k, v := range b.bySeverity {
			snap.BySeverity[k] += v
		}
		for k, v := range b.byArea {
			areas[k] += v
		}
	}
	for a, n := range areas {
		snap.TopAreas = append(snap.TopAreas, AreaCount{Area: a, Count: n})
	}
	sort.Slice(snap.TopAreas, func(i, j int) bool {
		if snap.TopAreas[i].Count != snap.TopAreas[j].Count {
			return snap.TopAreas[i].Count > snap.TopAreas[j].Count
		}
		return snap.TopAreas[i].Area < snap.TopAreas[j].Area
	})
	if len(snap.TopAreas) > TopAreasLimit {
		snap.TopAreas = snap.TopAreas[:TopAreasLimit]
	}
	return snap
}
