package anomaly

import (
	"math"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newDetector() (*Detector, *clock) {
	c := &clock{t: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
	return New(DefaultConfig(), c.now), c
}

func feed(d *Detector, c *clock, key string, perMinute []int) {
	for _, n := range perMinute {
		for i := 0; i < n; i++ {
			d.Observe(key)
		}
		c.advance(time.Minute)
	}
}

func steady(n, rate int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = rate
	}
	return out
}

func TestPoissonTailMatchesKnownValues(t *testing.T) {
	tests := []struct {
		k      int
		lambda float64
		want   float64
	}{
		{1, 3, 1 - math.Exp(-3)},
		{10, 3, 0.0011024},
		{5, 5, 0.5595067},
		{0, 3, 1},
	}
	for _, tt := range tests {
		got := PoissonTail(tt.k, tt.lambda)
		if math.Abs(got-tt.want) > 1e-5 {
			t.Fatalf("PoissonTail(%d,%v) = %v, want %v", tt.k, tt.lambda, got, tt.want)
		}
	}
	if p := PoissonTail(60, 3); p <= 0 || p > 1e-30 {
		t.Fatalf("ekor sangat jauh harus positif dan sangat kecil, dapat %v", p)
	}
}

func TestSteadyRateRaisesNoAnomaly(t *testing.T) {
	d, c := newDetector()
	for i := 0; i < 40; i++ {
		for j := 0; j < 3+i%2; j++ {
			d.Observe("type:WILDFIRE")
		}
		if got := d.Evaluate(); len(got) != 0 {
			t.Fatalf("menit %d: laju stabil menghasilkan anomali: %+v", i, got)
		}
		c.advance(time.Minute)
	}
}

func TestSuddenSpikeIsDetected(t *testing.T) {
	d, c := newDetector()
	feed(d, c, "type:WILDFIRE|area:Riau", steady(30, 3))

	for i := 0; i < 40; i++ {
		d.Observe("type:WILDFIRE|area:Riau")
	}
	got := d.Evaluate()
	if len(got) != 1 || got[0].Key != "type:WILDFIRE|area:Riau" || got[0].Count != 40 {
		t.Fatalf("lonjakan 3 -> 40 per menit seharusnya terdeteksi: %+v", got)
	}
	if got[0].Expected < 2 || got[0].Expected > 4 || got[0].PValue > 1e-10 {
		t.Fatalf("laju normal ~3 dan p sangat kecil, dapat %+v", got[0])
	}
}

func TestNothingIsFlaggedDuringWarmup(t *testing.T) {
	d, c := newDetector()
	feed(d, c, "k", steady(3, 2))
	for i := 0; i < 50; i++ {
		d.Observe("k")
	}
	if got := d.Evaluate(); len(got) != 0 {
		t.Fatalf("selama warmup tidak boleh ada anomali: %+v", got)
	}
}

func TestSmallBurstBelowMinCountIsIgnored(t *testing.T) {
	d, c := newDetector()
	feed(d, c, "k", steady(30, 0))
	for i := 0; i < 4; i++ {
		d.Observe("k")
	}
	if got := d.Evaluate(); len(got) != 0 {
		t.Fatalf("4 kejadian di bawah MinCount tidak boleh dianggap anomali: %+v", got)
	}
}

func TestQuietSeriesBecomesAnomalousOnlyForAClearBurst(t *testing.T) {
	d, c := newDetector()
	feed(d, c, "k", steady(30, 0))
	for i := 0; i < 15; i++ {
		d.Observe("k")
	}
	if got := d.Evaluate(); len(got) != 1 {
		t.Fatalf("15 kejadian pada deret yang biasanya sepi harus terdeteksi: %+v", got)
	}
}

func TestBaselineAdaptsToASustainedNewLevel(t *testing.T) {
	d, c := newDetector()
	feed(d, c, "k", steady(30, 3))
	feed(d, c, "k", steady(60, 20))
	for i := 0; i < 20; i++ {
		d.Observe("k")
	}
	if got := d.Evaluate(); len(got) != 0 {
		t.Fatalf("setelah laju baru bertahan lama, 20/menit sudah normal: %+v", got)
	}
}

func TestLongGapDecaysTheBaselineWithoutLooping(t *testing.T) {
	d, c := newDetector()
	feed(d, c, "k", steady(30, 10))
	c.advance(24 * time.Hour)
	start := time.Now()
	d.Observe("k")
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("jeda panjang tidak boleh membuat pelipatan bucket berjalan lama")
	}
}

func TestIdleSeriesArePrunedAndKeyCardinalityIsBounded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxKeys = 50
	c := &clock{t: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
	d := New(cfg, c.now)
	for i := 0; i < 500; i++ {
		d.Observe("key-" + string(rune('A'+i%26)) + string(rune('a'+i/26)))
	}
	if d.Len() > 50 {
		t.Fatalf("jumlah deret = %d, melebihi batas 50", d.Len())
	}

	c.advance(time.Minute)
	for i := 0; i < 500; i++ {
		c.advance(time.Minute)
		d.Evaluate()
	}
	if d.Len() != 0 {
		t.Fatalf("deret yang lama sepi harus dibuang, tersisa %d", d.Len())
	}
}

func TestBrandNewSeriesIsEvaluatedOnceTheDetectorHasRunLongEnough(t *testing.T) {
	d, c := newDetector()
	feed(d, c, "a", steady(30, 3))
	for i := 0; i < 60; i++ {
		d.Observe("b")
	}
	got := d.Evaluate()
	if len(got) != 1 || got[0].Key != "b" {
		t.Fatalf("lonjakan pada kunci baru harus terdeteksi, dan deret a yang stabil tidak: %+v", got)
	}
}

func TestBrandNewSeriesRightAfterStartIsNotJudged(t *testing.T) {
	d, _ := newDetector()
	for i := 0; i < 60; i++ {
		d.Observe("b")
	}
	if got := d.Evaluate(); len(got) != 0 {
		t.Fatalf("tepat setelah start tidak ada dasar pembanding, tidak boleh ada anomali: %+v", got)
	}
}
