package anomaly

import (
	"math"
	"sort"
	"sync"
	"time"
)

type Config struct {
	Alpha         float64
	WarmupBuckets int
	MinCount      int
	PThreshold    float64
	MinMean       float64
	Bucket        time.Duration
	MaxKeys       int
}

func DefaultConfig() Config {
	return Config{Alpha: 0.1, WarmupBuckets: 10, MinCount: 5, PThreshold: 1e-4, MinMean: 0.2, Bucket: time.Minute, MaxKeys: 2000}
}

type Anomaly struct {
	Key      string    `json:"key"`
	Count    int       `json:"count"`
	Expected float64   `json:"expected"`
	PValue   float64   `json:"p_value"`
	At       time.Time `json:"at"`
}

type series struct {
	mean   float64
	seen   int
	bucket int64
	count  int
}

type Detector struct {
	cfg      Config
	now      func() time.Time
	startIdx int64
	mu       sync.Mutex
	series   map[string]*series
}

func New(cfg Config, now func() time.Time) *Detector {
	if now == nil {
		now = time.Now
	}
	d := &Detector{cfg: cfg, now: now, series: map[string]*series{}}
	d.startIdx = d.index()
	return d
}

func (d *Detector) index() int64 { return d.now().UnixNano() / int64(d.cfg.Bucket) }

const maxGapFold = 120

func (d *Detector) roll(s *series, idx int64) {
	if idx <= s.bucket {
		return
	}
	fold := func(x float64) {
		if s.seen == 0 {
			s.mean = x
		} else {
			s.mean = d.cfg.Alpha*x + (1-d.cfg.Alpha)*s.mean
		}
		s.seen++
	}
	fold(float64(s.count))
	gap := idx - s.bucket - 1
	if gap > maxGapFold {
		gap = maxGapFold
	}
	for i := int64(0); i < gap; i++ {
		fold(0)
	}
	s.bucket, s.count = idx, 0
}

func (d *Detector) Observe(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	idx := d.index()
	s, ok := d.series[key]
	if !ok {
		if len(d.series) >= d.cfg.MaxKeys {
			return
		}
		known := int(min(idx-d.startIdx, int64(d.cfg.WarmupBuckets)))
		s = &series{bucket: idx, seen: max(known, 0)}
		d.series[key] = s
	}
	d.roll(s, idx)
	s.count++
}

func (d *Detector) Evaluate() []Anomaly {
	d.mu.Lock()
	defer d.mu.Unlock()
	idx := d.index()
	now := d.now()
	var out []Anomaly
	for key, s := range d.series {
		d.roll(s, idx)
		if s.seen >= d.cfg.WarmupBuckets && s.count >= d.cfg.MinCount {
			mean := math.Max(s.mean, d.cfg.MinMean)
			if p := PoissonTail(s.count, mean); p < d.cfg.PThreshold {
				out = append(out, Anomaly{Key: key, Count: s.count, Expected: s.mean, PValue: p, At: now})
			}
		}
		if s.count == 0 && s.mean < 0.01 && s.seen > d.cfg.WarmupBuckets {
			delete(d.series, key)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PValue != out[j].PValue {
			return out[i].PValue < out[j].PValue
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func (d *Detector) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.series)
}

func PoissonTail(k int, lambda float64) float64 {
	if k <= 0 {
		return 1
	}
	if lambda <= 0 {
		return 0
	}
	lg, _ := math.Lgamma(float64(k) + 1)
	term := math.Exp(-lambda + float64(k)*math.Log(lambda) - lg)
	sum := term
	for i := k + 1; i < k+2000; i++ {
		term *= lambda / float64(i)
		sum += term
		if term < sum*1e-15 {
			break
		}
	}
	return math.Min(sum, 1)
}
