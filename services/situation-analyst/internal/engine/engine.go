package engine

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"situation-analyst/internal/anomaly"
	"situation-analyst/internal/hazard"
	"situation-analyst/internal/llm"
	"situation-analyst/internal/summary"
	"situation-analyst/internal/window"
	"telemetry"
)

const Disclaimer = "Analisis otomatis sebagai saran. Bukan data resmi dan tidak memengaruhi peringatan."

type Summarizer interface {
	Enabled() bool
	Summarize(ctx context.Context, system, user string) (string, llm.Outcome, error)
	BreakerOpen() bool
}

type Config struct {
	WindowMinutes    int
	RetainMinutes    int
	SummaryInterval  time.Duration
	EvaluateInterval time.Duration
	AnomalyTTL       time.Duration
	TriggerCooldown  time.Duration
	LLMDeadline      time.Duration
	Anomaly          anomaly.Config
}

func (c *Config) defaults() {
	if c.WindowMinutes <= 0 {
		c.WindowMinutes = 15
	}
	if c.RetainMinutes < c.WindowMinutes {
		c.RetainMinutes = max(60, c.WindowMinutes)
	}
	if c.SummaryInterval <= 0 {
		c.SummaryInterval = 3 * time.Minute
	}
	if c.EvaluateInterval <= 0 {
		c.EvaluateInterval = 10 * time.Second
	}
	if c.AnomalyTTL <= 0 {
		c.AnomalyTTL = 10 * time.Minute
	}
	if c.TriggerCooldown <= 0 {
		c.TriggerCooldown = time.Minute
	}
	if c.LLMDeadline <= 0 {
		c.LLMDeadline = 30 * time.Second
	}
	if c.Anomaly.Bucket == 0 {
		c.Anomaly = anomaly.DefaultConfig()
	}
}

type LLMStatus struct {
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

type AnomalyView struct {
	Description string    `json:"description"`
	Count       int       `json:"count"`
	Expected    float64   `json:"expected_per_minute"`
	PValue      float64   `json:"p_value"`
	DetectedAt  time.Time `json:"detected_at"`
}

type Result struct {
	Available   bool            `json:"available"`
	GeneratedAt time.Time       `json:"generated_at"`
	Source      string          `json:"source"`
	Summary     string          `json:"summary"`
	Window      window.Snapshot `json:"window"`
	Anomalies   []AnomalyView   `json:"anomalies"`
	LLM         LLMStatus       `json:"llm"`
	Disclaimer  string          `json:"disclaimer"`
}

var (
	eventsSeen    = telemetry.Counter("analyst_events", "event yang diterima situation-analyst, per kategori")
	anomaliesSeen = telemetry.Counter("analyst_anomalies_detected", "anomali baru yang terdeteksi")
	cycles        = telemetry.Counter("analyst_cycles", "siklus analisis, per sumber ringkasan")
)

var knownCategories = map[string]bool{"SEISMIC": true, "TSUNAMI": true, "VOLCANIC": true, "WILDFIRE": true, "COASTAL": true}
var knownSeverities = map[string]bool{"NORMAL": true, "WASPADA": true, "SIAGA": true, "AWAS": true}

type Engine struct {
	cfg Config
	sum Summarizer
	log *slog.Logger
	now func() time.Time
	win *window.Window
	det *anomaly.Detector

	latest  atomic.Pointer[Result]
	running atomic.Bool

	mu          sync.Mutex
	active      map[string]anomaly.Anomaly
	lastTrigger time.Time
	trigger     chan struct{}
}

func New(cfg Config, sum Summarizer, log *slog.Logger, now func() time.Time) *Engine {
	cfg.defaults()
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}
	return &Engine{cfg: cfg, sum: sum, log: log, now: now,
		win:     window.New(cfg.RetainMinutes, now),
		det:     anomaly.New(cfg.Anomaly, now),
		active:  map[string]anomaly.Anomaly{},
		trigger: make(chan struct{}, 1)}
}

func (e *Engine) Latest() *Result { return e.latest.Load() }

func (e *Engine) ActiveAnomalies() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.active)
}

func normalize(v string, known map[string]bool) string {
	if known[v] {
		return v
	}
	return "OTHER"
}

func (e *Engine) Observe(ev hazard.Event) {
	cat := normalize(ev.Category(), knownCategories)
	sev := normalize(ev.SeverityName(), knownSeverities)
	area := ev.Area()

	e.win.Add(cat, sev, area)
	e.det.Observe(summary.CategoryKey(cat))
	e.det.Observe(summary.AreaKey(cat, area))
	if sev == "AWAS" || sev == "SIAGA" {
		e.det.Observe(summary.SeverityKey(sev))
	}
	eventsSeen(context.Background(), 1, "category", cat)
}

func (e *Engine) Evaluate() []anomaly.Anomaly {
	found := e.det.Evaluate()
	now := e.now()

	e.mu.Lock()
	defer e.mu.Unlock()
	fresh := false
	for _, a := range found {
		if _, known := e.active[a.Key]; !known {
			fresh = true
			anomaliesSeen(context.Background(), 1)
		}
		e.active[a.Key] = a
	}
	for k, a := range e.active {
		if now.Sub(a.At) > e.cfg.AnomalyTTL {
			delete(e.active, k)
		}
	}
	if fresh && now.Sub(e.lastTrigger) >= e.cfg.TriggerCooldown {
		e.lastTrigger = now
		select {
		case e.trigger <- struct{}{}:
		default:
		}
	}
	return e.activeLocked()
}

func (e *Engine) activeLocked() []anomaly.Anomaly {
	out := make([]anomaly.Anomaly, 0, len(e.active))
	for _, a := range e.active {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PValue != out[j].PValue {
			return out[i].PValue < out[j].PValue
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func (e *Engine) Cycle(ctx context.Context) *Result {
	e.mu.Lock()
	an := e.activeLocked()
	e.mu.Unlock()
	snap := e.win.Snapshot(e.cfg.WindowMinutes)

	res := &Result{
		Available:   true,
		GeneratedAt: e.now().UTC(),
		Source:      "template",
		Summary:     summary.Template(snap, an),
		Window:      snap,
		Anomalies:   views(an),
		LLM:         LLMStatus{Status: string(llm.OutcomeDisabled)},
		Disclaimer:  Disclaimer,
	}

	switch {
	case !e.sum.Enabled():
	case snap.Total == 0 && len(an) == 0:
		res.LLM = LLMStatus{Status: "idle", Detail: "tidak ada kejadian pada jendela ini, LLM tidak dipanggil"}
	default:
		e.askLLM(ctx, res, snap, an)
	}

	e.latest.Store(res)
	cycles(ctx, 1, "source", res.Source)
	return res
}

func (e *Engine) askLLM(ctx context.Context, res *Result, snap window.Snapshot, an []anomaly.Anomaly) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.LLMDeadline)
	defer cancel()
	system, user := summary.Prompt(snap, an)
	text, outcome, err := e.sum.Summarize(ctx, system, user)
	if outcome != llm.OutcomeOK {
		res.LLM = LLMStatus{Status: string(outcome)}
		if err != nil {
			res.LLM.Detail = err.Error()
		}
		return
	}
	valid, verr := summary.ValidateOutput(text, summary.AllowedNumbers(snap, an))
	if verr != nil {
		res.LLM = LLMStatus{Status: "invalid_output", Detail: verr.Error()}
		e.log.Warn("keluaran LLM ditolak", "alasan", verr.Error())
		return
	}
	res.Source, res.Summary, res.LLM = "llm", valid, LLMStatus{Status: "ok"}
}

func views(an []anomaly.Anomaly) []AnomalyView {
	out := make([]AnomalyView, 0, len(an))
	for _, a := range an {
		out = append(out, AnomalyView{Description: summary.Describe(a.Key), Count: a.Count,
			Expected: float64(int(a.Expected*10+0.5)) / 10, PValue: a.PValue, DetectedAt: a.At.UTC()})
	}
	return out
}

func (e *Engine) Run(ctx context.Context) {
	e.startCycle(ctx)

	evalT := time.NewTicker(e.cfg.EvaluateInterval)
	defer evalT.Stop()
	sumT := time.NewTicker(e.cfg.SummaryInterval)
	defer sumT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-evalT.C:
			e.Evaluate()
		case <-sumT.C:
			e.startCycle(ctx)
		case <-e.trigger:
			e.startCycle(ctx)
		}
	}
}

func (e *Engine) startCycle(ctx context.Context) {
	if !e.running.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer e.running.Store(false)
		e.Cycle(ctx)
	}()
}

func Describe(key string) string { return strings.TrimSpace(summary.Describe(key)) }
