package fire

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"time"

	"satellite-ingestor/internal/detection"
	"satellite-ingestor/internal/hazard"
	"satellite-ingestor/internal/province"
)

const (
	MaxDistanceKM = 1.0
	MaxGap        = 360 * time.Minute
	cellDeg       = 0.01
)

type Outcome string

const (
	Accepted         Outcome = "accepted"
	Duplicate        Outcome = "duplicate"
	Late             Outcome = "late"
	LowConfidence    Outcome = "low_confidence"
	Updated          Outcome = "updated"
	Retracted        Outcome = "retracted"
	RetractUnmatched Outcome = "retract_unmatched"
)

type Stats map[string]int64

type Changes struct {
	PutDetections []detection.Detection
	FinalDetections  []detection.Detection
	DeleteDetections []string
	Late             []detection.Detection
	UpsertFires      []hazard.Event
	DeleteFires      []string
	FinalizedFires []string
}

type Engine struct {
	lateness  time.Duration
	provinces *province.Table
	now       func() time.Time

	active     map[string]detection.Detection // deteksi non-low yang belum final
	maxEvent   time.Time
	virtualNow time.Time
	emitted    map[string]string // hazard_id kebakaran aktif -> fingerprint
	stats      Stats

	dirtyPut map[string]detection.Detection
	dirtyDel map[string]bool
	late     []detection.Detection

	frozen map[string]detection.Detection
	groups map[string][]string
}

func New(lateness time.Duration, provinces *province.Table) *Engine {
	return &Engine{
		lateness:  lateness,
		provinces: provinces,
		now:       time.Now,
		active:    map[string]detection.Detection{},
		emitted:   map[string]string{},
		stats:     Stats{},
		dirtyPut:  map[string]detection.Detection{},
		dirtyDel:  map[string]bool{},
		frozen:    map[string]detection.Detection{},
		groups:    map[string][]string{},
	}
}

func (e *Engine) freeze(group string, members []detection.Detection) []detection.Detection {
	out := make([]detection.Detection, 0, len(members))
	keys := make([]string, 0, len(members))
	for _, m := range members {
		m.Group = group
		delete(e.active, m.Key)
		delete(e.dirtyPut, m.Key)
		e.frozen[m.Key] = m
		keys = append(keys, m.Key)
		out = append(out, m)
	}
	e.groups[group] = keys
	return out
}

// mencairkan kelompok tempat key dibekukan. Kebakaran yang dicairkan dimasukkan
// kembali ke daftar kebakaran aktif dengan fingerprint kosong, sehingga Flush berikutnya
// memperbarui atau menghapus HazardEvent-nya sesuai keadaan baru.
func (e *Engine) thaw(key string) bool {
	d, ok := e.frozen[key]
	if !ok {
		return false
	}
	for _, k := range e.groups[d.Group] {
		m := e.frozen[k]
		delete(e.frozen, k)
		m.Group = ""
		e.active[k] = m
		e.dirtyPut[k] = m
	}
	delete(e.groups, d.Group)
	if len(d.Group) > 3 && d.Group[:3] == "hz-" {
		e.emitted[d.Group] = ""
	}
	e.count("thawed")
	return true
}

// batas waktu deteksi yang masih diterima.
func (e *Engine) Watermark() time.Time {
	ref := e.maxEvent
	if e.virtualNow.After(ref) {
		ref = e.virtualNow
	}
	if ref.IsZero() {
		return time.Time{}
	}
	return ref.Add(-e.lateness)
}

func (e *Engine) MaxEventTime() time.Time { return e.maxEvent }
func (e *Engine) VirtualNow() time.Time   { return e.virtualNow }
func (e *Engine) Stats() Stats {
	out := Stats{}
	for k, v := range e.stats {
		out[k] = v
	}
	return out
}
func (e *Engine) ActiveFires() map[string]string {
	out := make(map[string]string, len(e.emitted))
	for k, v := range e.emitted {
		out[k] = v
	}
	return out
}
func (e *Engine) ActiveDetections() int { return len(e.active) }

func (e *Engine) ObserveVirtualNow(t time.Time) {
	if t.After(e.virtualNow) {
		e.virtualNow = t
	}
}

func (e *Engine) count(o Outcome) { e.stats[string(o)]++ }

func (e *Engine) isLate(d detection.Detection) bool {
	w := e.Watermark()
	return !w.IsZero() && d.Time.Before(w)
}

func (e *Engine) markLate(d detection.Detection) Outcome {
	e.late = append(e.late, d)
	e.count(Late)
	return Late
}

func (e *Engine) Add(d detection.Detection) Outcome {
	_, frozen := e.frozen[d.Key]
	if _, ok := e.active[d.Key]; ok || frozen {
		e.count(Duplicate)
		return Duplicate
	}
	if e.isLate(d) {
		return e.markLate(d)
	}
	if d.Time.After(e.maxEvent) {
		e.maxEvent = d.Time
	}
	if d.Low() {
		e.count(LowConfidence)
		return LowConfidence
	}
	e.active[d.Key] = d
	e.dirtyPut[d.Key] = d
	delete(e.dirtyDel, d.Key)
	e.count(Accepted)
	return Accepted
}

func (e *Engine) Update(d detection.Detection) Outcome {
	e.thaw(d.Key)
	_, exists := e.active[d.Key]
	if !exists {
		if e.isLate(d) {
			return e.markLate(d)
		}
		if d.Low() {
			e.count(LowConfidence)
			return LowConfidence
		}
		e.active[d.Key] = d
		e.dirtyPut[d.Key] = d
		delete(e.dirtyDel, d.Key)
		e.count(Updated)
		return Updated
	}
	if d.Low() {
		// Confidence turun menjadi low: deteksi keluar dari pembentukan kebakaran.
		delete(e.active, d.Key)
		delete(e.dirtyPut, d.Key)
		e.dirtyDel[d.Key] = true
		e.count(Updated)
		return Updated
	}
	// Offset pertama dipertahankan: pemulihan memuat deteksi menurut offset saat ia
	// pertama diterima, sehingga versi yang diperbarui sesudah checkpoint tetap termuat.
	d.Offset = e.active[d.Key].Offset
	e.active[d.Key] = d
	e.dirtyPut[d.Key] = d
	e.count(Updated)
	return Updated
}

func (e *Engine) Retract(key string) Outcome {
	e.thaw(key)
	if _, ok := e.active[key]; !ok {
		e.count(RetractUnmatched)
		return RetractUnmatched
	}
	delete(e.active, key)
	delete(e.dirtyPut, key)
	e.dirtyDel[key] = true
	e.count(Retracted)
	return Retracted
}

func (e *Engine) Restore(dets []detection.Detection, maxEvent, virtualNow time.Time, emitted map[string]string, stats Stats) {
	e.active = map[string]detection.Detection{}
	e.frozen = map[string]detection.Detection{}
	e.groups = map[string][]string{}
	for _, d := range dets {
		if d.Group != "" {
			e.frozen[d.Key] = d
			e.groups[d.Group] = append(e.groups[d.Group], d.Key)
			continue
		}
		e.active[d.Key] = d
	}
	e.maxEvent, e.virtualNow = maxEvent, virtualNow
	e.emitted = map[string]string{}
	for k, v := range emitted {
		e.emitted[k] = v
	}
	e.stats = Stats{}
	for k, v := range stats {
		e.stats[k] = v
	}
}

type cellKey struct{ x, y int64 }

func cellOf(lat, lon float64) cellKey {
	return cellKey{int64(math.Floor(lat / cellDeg)), int64(math.Floor(lon / cellDeg))}
}

// mengelompokkan deteksi aktif menjadi komponen terhubung, terurut stabil.
func Components(dets map[string]detection.Detection) [][]detection.Detection {
	keys := make([]string, 0, len(dets))
	for k := range dets {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	idx := make(map[string]int, len(keys))
	grid := map[cellKey][]int{}
	for i, k := range keys {
		idx[k] = i
		c := cellOf(dets[k].Lat, dets[k].Lon)
		grid[c] = append(grid[c], i)
	}
	parent := make([]int, len(keys))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(i int) int {
		for parent[i] != i {
			parent[i] = parent[parent[i]]
			i = parent[i]
		}
		return i
	}
	union := func(a, b int) {
		ra, rb := find(a), find(b)
		if ra != rb {
			if ra < rb {
				parent[rb] = ra
			} else {
				parent[ra] = rb
			}
		}
	}
	for i, k := range keys {
		d := dets[k]
		c := cellOf(d.Lat, d.Lon)
		// Lebar sel bujur menyusut menurut cos(lintang). pastikan radius 1 km tercakup.
		kmPerCellLon := 111.32 * cellDeg * math.Cos(d.Lat*math.Pi/180)
		span := int64(1)
		if kmPerCellLon > 0 {
			span = int64(math.Ceil(MaxDistanceKM / kmPerCellLon))
		}
		for dx := int64(-1); dx <= 1; dx++ {
			for dy := -span; dy <= span; dy++ {
				for _, j := range grid[cellKey{c.x + dx, c.y + dy}] {
					if j <= i {
						continue
					}
					o := dets[keys[j]]
					gap := d.Time.Sub(o.Time)
					if gap < 0 {
						gap = -gap
					}
					if gap > MaxGap {
						continue
					}
					if province.HaversineKM(d.Lat, d.Lon, o.Lat, o.Lon) <= MaxDistanceKM {
						union(i, j)
					}
				}
			}
		}
	}
	groups := map[int][]detection.Detection{}
	for i, k := range keys {
		r := find(i)
		groups[r] = append(groups[r], dets[k])
	}
	roots := make([]int, 0, len(groups))
	for r := range groups {
		roots = append(roots, r)
	}
	sort.Ints(roots)
	out := make([][]detection.Detection, 0, len(roots))
	for _, r := range roots {
		out = append(out, groups[r])
	}
	return out
}

func earliest(members []detection.Detection) detection.Detection {
	best := members[0]
	for _, m := range members[1:] {
		if m.Time.Before(best.Time) || (m.Time.Equal(best.Time) && m.Key < best.Key) {
			best = m
		}
	}
	return best
}

func FireID(members []detection.Detection) string {
	sum := sha256.Sum256([]byte(earliest(members).Key))
	return "fire-" + hex.EncodeToString(sum[:])[:12]
}

func Severity(count int, frp float64) string {
	switch {
	case count >= 20 || frp >= 100:
		return "AWAS"
	case count >= 6 || frp >= 30:
		return "SIAGA"
	default:
		return "WASPADA"
	}
}

var confRank = map[string]int{"l": 0, "n": 1, "h": 2}

// memetakan satu kebakaran ke HazardEvent WILDFIRE (tanpa ingested_at).
func ToEvent(members []detection.Detection, provinces *province.Table) hazard.Event {
	first := earliest(members)
	var sumLat, sumLon, frp float64
	sats := map[string]bool{}
	maxConf, maxConfRaw := -1, ""
	keys := make([]string, 0, len(members))
	for _, m := range members {
		sumLat += m.Lat
		sumLon += m.Lon
		frp += m.FRP
		sats[m.Satellite] = true
		if r := confRank[m.Confidence]; r > maxConf {
			maxConf, maxConfRaw = r, m.Confidence
		}
		keys = append(keys, m.Key)
	}
	sort.Strings(keys)
	satList := make([]string, 0, len(sats))
	for s := range sats {
		satList = append(satList, s)
	}
	sort.Strings(satList)
	n := float64(len(members))
	lat := math.Round(sumLat/n*1e5) / 1e5
	lon := math.Round(sumLon/n*1e5) / 1e5
	frp = math.Round(frp*100) / 100
	id := FireID(members)
	return hazard.Event{
		HazardID:    "hz-" + id,
		Source:      hazard.SourceSatelit,
		SourceRefID: id,
		HazardType:  hazard.TypeWildfire,
		Severity:    Severity(len(members), frp),
		AreaName:    provinces.Nearest(lat, lon),
		Latitude:    lat,
		Longitude:   lon,
		OccurredAt:  first.Time,
		Attributes: map[string]any{
			"detection_count": len(members),
			"satellites":      satList,
			"max_confidence":  maxConfRaw,
			"total_frp_mw":    frp,
			"detection_keys":  keys,
		},
	}
}

func fingerprint(ev hazard.Event) string {
	ev.IngestedAt = time.Time{}
	b, _ := json.Marshal(ev)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
