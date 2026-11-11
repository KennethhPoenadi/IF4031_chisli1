package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"encoding/json"

	"situation-analyst/internal/hazard"
	"situation-analyst/internal/llm"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClock() *clock { return &clock{t: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)} }

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type fakeLLM struct {
	enabled bool
	calls   atomic.Int32
	fn      func(ctx context.Context, system, user string) (string, llm.Outcome, error)
}

func (f *fakeLLM) Enabled() bool     { return f.enabled }
func (f *fakeLLM) BreakerOpen() bool { return false }
func (f *fakeLLM) Summarize(ctx context.Context, system, user string) (string, llm.Outcome, error) {
	f.calls.Add(1)
	return f.fn(ctx, system, user)
}

func disabled() *fakeLLM { return &fakeLLM{enabled: false} }

func ev(typ, sev, area string) hazard.Event {
	return hazard.Event{HazardID: "hz-x", HazardType: typ, Severity: sev, AreaName: area}
}

func feed(e *Engine, n int, typ, sev, area string) {
	for i := 0; i < n; i++ {
		e.Observe(ev(typ, sev, area))
	}
}

func TestTemplateResultWhenLLMIsDisabled(t *testing.T) {
	clk := newClock()
	e := New(Config{}, disabled(), quiet(), clk.now)
	feed(e, 6, "WILDFIRE", "AWAS", "Riau")
	feed(e, 2, "SEISMIC", "NORMAL", "Aceh")

	res := e.Cycle(context.Background())
	if res.Source != "template" || res.LLM.Status != "disabled" || !res.Available {
		t.Fatalf("res = %+v", res)
	}
	if res.Window.Total != 8 || res.Window.ByCategory["WILDFIRE"] != 6 || res.Window.BySeverity["AWAS"] != 6 {
		t.Fatalf("window = %+v", res.Window)
	}
	if !strings.Contains(res.Summary, "8 kejadian") || !strings.Contains(res.Summary, "6 kebakaran hutan") {
		t.Fatalf("ringkasan = %q", res.Summary)
	}
	if res.Disclaimer == "" || e.Latest() != res {
		t.Fatal("hasil harus disimpan dan membawa disclaimer")
	}
}

func TestFaithfulLLMSummaryIsUsed(t *testing.T) {
	clk := newClock()
	f := &fakeLLM{enabled: true, fn: func(context.Context, string, string) (string, llm.Outcome, error) {
		return "Dalam 15 menit terakhir tercatat 8 kejadian, terbanyak kebakaran hutan (6).", llm.OutcomeOK, nil
	}}
	e := New(Config{}, f, quiet(), clk.now)
	feed(e, 6, "WILDFIRE", "AWAS", "Riau")
	feed(e, 2, "SEISMIC", "NORMAL", "Aceh")

	res := e.Cycle(context.Background())
	if res.Source != "llm" || res.LLM.Status != "ok" || !strings.Contains(res.Summary, "terbanyak kebakaran hutan") {
		t.Fatalf("res = %+v", res)
	}
}

func TestWrongLLMOutputFallsBackToTheAccurateTemplate(t *testing.T) {
	clk := newClock()
	f := &fakeLLM{enabled: true, fn: func(context.Context, string, string) (string, llm.Outcome, error) {
		return "Terjadi 9999 kejadian dan sebaiknya segera evakuasi seluruh warga.", llm.OutcomeOK, nil
	}}
	e := New(Config{}, f, quiet(), clk.now)
	feed(e, 8, "WILDFIRE", "AWAS", "Riau")

	res := e.Cycle(context.Background())
	if res.Source != "template" || res.LLM.Status != "invalid_output" {
		t.Fatalf("angka karangan harus ditolak: %+v", res)
	}
	if strings.Contains(res.Summary, "9999") || strings.Contains(res.Summary, "evakuasi") {
		t.Fatalf("teks salah dari LLM tidak boleh sampai ke pengguna: %q", res.Summary)
	}
}

func TestEveryLLMFailureModeStillYieldsAUsableResult(t *testing.T) {
	cases := map[string]struct {
		outcome llm.Outcome
		err     error
	}{
		"timeout":      {llm.OutcomeTimeout, errors.New("melewati batas waktu")},
		"error":        {llm.OutcomeError, errors.New("LLM menjawab status 500")},
		"breaker":      {llm.OutcomeBreakerOpen, errors.New("breaker terbuka")},
		"budget":       {llm.OutcomeBudget, errors.New("anggaran habis")},
		"bad_response": {llm.OutcomeBadResponse, errors.New("bukan json")},
	}
	for name, tc := range cases {
		f := &fakeLLM{enabled: true, fn: func(context.Context, string, string) (string, llm.Outcome, error) {
			return "", tc.outcome, tc.err
		}}
		e := New(Config{}, f, quiet(), newClock().now)
		feed(e, 7, "COASTAL", "SIAGA", "Bali")
		res := e.Cycle(context.Background())
		if res.Source != "template" || res.LLM.Status != string(tc.outcome) || res.Summary == "" || !res.Available {
			t.Fatalf("%s: res = %+v", name, res)
		}
		if res.LLM.Detail != tc.err.Error() {
			t.Fatalf("%s: detail = %q", name, res.LLM.Detail)
		}
	}
}

func TestIdleWindowDoesNotSpendLLMQuota(t *testing.T) {
	f := &fakeLLM{enabled: true, fn: func(context.Context, string, string) (string, llm.Outcome, error) {
		return "x", llm.OutcomeOK, nil
	}}
	e := New(Config{}, f, quiet(), newClock().now)
	res := e.Cycle(context.Background())
	if f.calls.Load() != 0 || res.LLM.Status != "idle" || !strings.Contains(res.Summary, "belum ada kejadian") {
		t.Fatalf("calls=%d res=%+v", f.calls.Load(), res)
	}
}

func TestAHangingLLMDoesNotSlowDownEventProcessing(t *testing.T) {
	started := make(chan struct{})
	f := &fakeLLM{enabled: true, fn: func(ctx context.Context, _, _ string) (string, llm.Outcome, error) {
		close(started)
		<-ctx.Done()
		return "", llm.OutcomeTimeout, ctx.Err()
	}}
	e := New(Config{LLMDeadline: 400 * time.Millisecond}, f, quiet(), newClock().now)
	feed(e, 10, "WILDFIRE", "AWAS", "Riau")

	observe := func() (total, worst time.Duration) {
		begin := time.Now()
		for i := 0; i < 20000; i++ {
			t0 := time.Now()
			e.Observe(ev("WILDFIRE", "SIAGA", fmt.Sprintf("Area-%d", i%50)))
			if d := time.Since(t0); d > worst {
				worst = d
			}
		}
		return time.Since(begin), worst
	}
	baseline, _ := observe()

	done := make(chan *Result, 1)
	go func() { done <- e.Cycle(context.Background()) }()
	<-started

	total, worst := observe()
	if worst > 250*time.Millisecond || total > 3*baseline+100*time.Millisecond {
		t.Fatalf("selama LLM macet 20000 event butuh %v (terlama %v), dasar %v: pemrosesan event terpengaruh", total, worst, baseline)
	}
	t.Logf("20000 event: %v saat LLM macet, %v tanpa LLM macet (terlama satu panggilan %v)", total, baseline, worst)

	select {
	case res := <-done:
		if res.Source != "template" || res.LLM.Status != "timeout" {
			t.Fatalf("setelah batas waktu hasil harus templat: %+v", res)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("siklus tidak berhenti pada batas waktu LLM")
	}
}

func TestCycleBoundsTheLLMEvenIfTheClientIgnoresItsOwnTimeout(t *testing.T) {
	f := &fakeLLM{enabled: true, fn: func(ctx context.Context, _, _ string) (string, llm.Outcome, error) {
		<-ctx.Done()
		return "", llm.OutcomeTimeout, ctx.Err()
	}}
	e := New(Config{LLMDeadline: 120 * time.Millisecond}, f, quiet(), newClock().now)
	feed(e, 6, "SEISMIC", "WASPADA", "Aceh")
	start := time.Now()
	e.Cycle(context.Background())
	if time.Since(start) > time.Second {
		t.Fatalf("siklus menunggu LLM %v, harus berhenti sekitar 120ms", time.Since(start))
	}
}

func TestSpikeIsDetectedAndTriggersASummary(t *testing.T) {
	clk := newClock()
	e := New(Config{TriggerCooldown: time.Second}, disabled(), quiet(), clk.now)
	for i := 0; i < 30; i++ {
		feed(e, 3, "WILDFIRE", "SIAGA", "Riau")
		clk.advance(time.Minute)
		e.Evaluate()
	}
	if e.ActiveAnomalies() != 0 {
		t.Fatalf("laju stabil tidak boleh menghasilkan anomali, aktif=%d", e.ActiveAnomalies())
	}
	select {
	case <-e.trigger:
		t.Fatal("tanpa anomali tidak boleh ada pemicu")
	default:
	}

	feed(e, 40, "WILDFIRE", "SIAGA", "Riau")
	found := e.Evaluate()
	if len(found) == 0 || e.ActiveAnomalies() == 0 {
		t.Fatalf("lonjakan harus terdeteksi: %+v", found)
	}
	select {
	case <-e.trigger:
	default:
		t.Fatal("anomali baru harus memicu ringkasan")
	}

	res := e.Cycle(context.Background())
	if len(res.Anomalies) == 0 || !strings.Contains(res.Summary, "Lonjakan tidak wajar") {
		t.Fatalf("hasil harus memuat anomali: %+v", res)
	}
	if !strings.Contains(res.Anomalies[0].Description, "kebakaran hutan") {
		t.Fatalf("deskripsi anomali = %q", res.Anomalies[0].Description)
	}

	clk.advance(15 * time.Minute)
	e.Evaluate()
	if e.ActiveAnomalies() != 0 {
		t.Fatalf("anomali harus kedaluwarsa, aktif=%d", e.ActiveAnomalies())
	}
}

func TestTriggerIsRateLimited(t *testing.T) {
	clk := newClock()
	e := New(Config{TriggerCooldown: time.Hour}, disabled(), quiet(), clk.now)
	for i := 0; i < 30; i++ {
		feed(e, 3, "WILDFIRE", "SIAGA", "Riau")
		clk.advance(time.Minute)
		e.Evaluate()
	}
	feed(e, 40, "WILDFIRE", "SIAGA", "Riau")
	e.Evaluate()
	<-e.trigger
	clk.advance(2 * time.Minute)
	feed(e, 40, "SEISMIC", "AWAS", "Aceh")
	e.Evaluate()
	select {
	case <-e.trigger:
		t.Fatal("pemicu kedua dalam masa jeda harus ditahan, agar kuota LLM tidak terkuras")
	default:
	}
}

func TestUnknownCategoriesAndSeveritiesCannotInflateCardinality(t *testing.T) {
	e := New(Config{}, disabled(), quiet(), newClock().now)
	for i := 0; i < 500; i++ {
		e.Observe(ev(fmt.Sprintf("JENIS-%d", i), fmt.Sprintf("TINGKAT-%d", i), "Riau"))
	}
	res := e.Cycle(context.Background())
	if len(res.Window.ByCategory) != 1 || res.Window.ByCategory["OTHER"] != 500 {
		t.Fatalf("kategori liar harus dilebur ke OTHER: %v", res.Window.ByCategory)
	}
	if len(res.Window.BySeverity) != 1 || res.Window.BySeverity["OTHER"] != 500 {
		t.Fatalf("tingkat liar harus dilebur ke OTHER: %v", res.Window.BySeverity)
	}
}

func TestTsunamiWarningsCountAsTheirOwnCategory(t *testing.T) {
	e := New(Config{}, disabled(), quiet(), newClock().now)
	warned := ev("SEISMIC", "AWAS", "Aceh")
	warned.Attributes = map[string]json.RawMessage{"tsunami_warning_threat_level": []byte(`"Awas"`)}
	e.Observe(warned)
	e.Observe(ev("SEISMIC", "NORMAL", "Aceh"))
	res := e.Cycle(context.Background())
	if res.Window.ByCategory["TSUNAMI"] != 1 || res.Window.ByCategory["SEISMIC"] != 1 {
		t.Fatalf("kategori = %v", res.Window.ByCategory)
	}
}

func TestRunProducesAnImmediateResultAndStopsOnCancel(t *testing.T) {
	e := New(Config{EvaluateInterval: 10 * time.Millisecond, SummaryInterval: 20 * time.Millisecond}, disabled(), quiet(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.Run(ctx); close(done) }()

	deadline := time.After(2 * time.Second)
	for e.Latest() == nil {
		select {
		case <-deadline:
			t.Fatal("Run harus segera menghasilkan hasil pertama")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run tidak berhenti setelah ctx dibatalkan")
	}
}

func TestOnlyOneCycleRunsAtATime(t *testing.T) {
	release := make(chan struct{})
	var concurrent, maxConcurrent atomic.Int32
	f := &fakeLLM{enabled: true, fn: func(ctx context.Context, _, _ string) (string, llm.Outcome, error) {
		n := concurrent.Add(1)
		for {
			m := maxConcurrent.Load()
			if n <= m || maxConcurrent.CompareAndSwap(m, n) {
				break
			}
		}
		<-release
		concurrent.Add(-1)
		return "", llm.OutcomeError, errors.New("x")
	}}
	e := New(Config{}, f, quiet(), newClock().now)
	feed(e, 6, "WILDFIRE", "AWAS", "Riau")
	for i := 0; i < 10; i++ {
		e.startCycle(context.Background())
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	time.Sleep(50 * time.Millisecond)
	if maxConcurrent.Load() != 1 {
		t.Fatalf("siklus berjalan bersamaan: %d", maxConcurrent.Load())
	}
}
