package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"satellite-ingestor/internal/aggclient"
	"satellite-ingestor/internal/detection"
	"satellite-ingestor/internal/feed"
	"satellite-ingestor/internal/fire"
	"telemetry"
)

type Store interface {
	Apply(ctx context.Context, b aggclient.Batch) error
	Checkpoint(ctx context.Context) (*aggclient.Checkpoint, error)
	ActiveDetections(ctx context.Context, maxOffset int64) ([]aggclient.Detection, bool, error)
}

type Source interface {
	Stream(ctx context.Context, last int64, handle func(feed.Event) error) (int64, error)
	Session(ctx context.Context) (feed.Session, error)
}

type Pipeline struct {
	Feed      Source
	Store     Store
	Engine    *fire.Engine
	BatchMax  int
	BatchWait time.Duration
	Log       *slog.Logger
	// LeaseToken diisi mekanisme leader election (fencing token) bila ada.
	LeaseToken func() string
	// AllowGap dipanggil ketika lompatan offset yang sama berulang.
	AllowGap func()

	committed    atomic.Int64
	latest       atomic.Int64
	ready        atomic.Bool
	unauthorized atomic.Bool
	sessionVNow  atomic.Int64
	maxEventUnix atomic.Int64
	stats        atomic.Value // fire.Stats

	pendingFinal []detection.Detection
	metricsOnce  sync.Once
}

var (
	eventsC    = telemetry.Counter("satellite_events", "kejadian Feed Satelit yang diterima, per jenis dan hasil")
	reconnects = telemetry.Counter("satellite_reconnects", "sambung ulang ke Feed Satelit, per alasan")
	gaps       = telemetry.Counter("satellite_offset_gaps", "lompatan offset feed yang terdeteksi")
	batchFail  = telemetry.Counter("satellite_batch_failures", "pengiriman batch ke Aggregator yang gagal dan diulang")
	firesC     = telemetry.Counter("satellite_fire_changes", "perubahan HazardEvent WILDFIRE, per operasi")
	ingestLat  = telemetry.Histogram("satellite_ingest_to_store_seconds", "detik dari kejadian diterima dari feed sampai HazardEvent tersimpan", 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300)
	batchSizeH = telemetry.Histogram("satellite_batch_events", "jumlah kejadian per batch", 1, 10, 50, 100, 250, 500, 1000)
)

func (p *Pipeline) registerMetrics() {
	p.metricsOnce.Do(func() {
		telemetry.GaugeFunc("satellite_committed_offset", "offset feed terakhir yang tersimpan", func() []telemetry.Sample {
			return []telemetry.Sample{{Value: float64(p.committed.Load())}}
		})
		telemetry.GaugeFunc("satellite_latest_offset", "offset terbaru di feed (dari /v1/session)", func() []telemetry.Sample {
			return []telemetry.Sample{{Value: float64(p.latest.Load())}}
		})
		telemetry.GaugeFunc("satellite_offset_lag", "jumlah kejadian feed yang belum tersimpan", func() []telemetry.Sample {
			lag := p.latest.Load() - p.committed.Load()
			return []telemetry.Sample{{Value: float64(max(lag, 0))}}
		})
		telemetry.GaugeFunc("satellite_event_time_lag_seconds", "selisih waktu virtual feed dengan waktu deteksi terbaru yang tersimpan", func() []telemetry.Sample {
			v, m := p.sessionVNow.Load(), p.maxEventUnix.Load()
			if v == 0 || m == 0 {
				return nil
			}
			return []telemetry.Sample{{Value: float64(max(v-m, 0))}}
		})
		telemetry.GaugeFunc("satellite_engine", "keadaan pembentukan kebakaran (kebakaran aktif, deteksi tunggal, terlambat, dsb.)", func() []telemetry.Sample {
			s, _ := p.stats.Load().(fire.Stats)
			out := make([]telemetry.Sample, 0, len(s))
			for k, v := range s {
				out = append(out, telemetry.Sample{Value: float64(v), Attrs: []any{"kind", k}})
			}
			return out
		})
	})
}

// Ready dipakai readiness probe.
func (p *Pipeline) Ready() bool { return p.ready.Load() && !p.unauthorized.Load() }

func (p *Pipeline) Committed() int64 { return p.committed.Load() }

// HealthHandler melaporkan keadaan tanpa membuat pod di-restart karena feed bermasalah.
func (p *Pipeline) HealthHandler() http.Handler {
	mux := http.NewServeMux()
	write := func(w http.ResponseWriter, code int) {
		s, _ := p.stats.Load().(fire.Stats)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ready": p.Ready(), "unauthorized": p.unauthorized.Load(),
			"committed_offset": p.committed.Load(), "latest_offset": p.latest.Load(), "stats": s,
		})
	}
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { write(w, http.StatusOK) })
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		if p.Ready() {
			write(w, http.StatusOK)
			return
		}
		write(w, http.StatusServiceUnavailable)
	})
	return mux
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d)/2+1))
}

// Recover memulihkan keadaan dari checkpoint. Mengembalikan offset terakhir yang
// tersimpan (-1 bila belum pernah ada).
func (p *Pipeline) Recover(ctx context.Context) (int64, error) {
	backoff := time.Second
	partialSince := time.Time{}
	for {
		start, err := p.recoverOnce(ctx, &partialSince)
		if err == nil {
			return start, nil
		}
		if ctx.Err() != nil {
			return -1, ctx.Err()
		}
		p.Log.Warn("pemulihan dari checkpoint gagal, dicoba lagi", "err", err, "backoff_ms", backoff.Milliseconds())
		if !sleepCtx(ctx, jitter(backoff)) {
			return -1, ctx.Err()
		}
		backoff = min(backoff*2, 15*time.Second)
	}
}

var errPartial = errors.New("sebagian unit penyimpanan tidak terjangkau")

func (p *Pipeline) recoverOnce(ctx context.Context, partialSince *time.Time) (int64, error) {
	cp, err := p.Store.Checkpoint(ctx)
	if err != nil {
		return -1, err
	}
	if cp == nil {
		p.committed.Store(-1)
		p.Log.Info("belum ada checkpoint satelit, mulai dari awal feed")
		return -1, nil
	}
	items, partial, err := p.Store.ActiveDetections(ctx, cp.Offset)
	if err != nil {
		return -1, err
	}
	if partial {
		// Deteksi yang hilang membuat rekonsiliasi salah; tunggu sebentar sampai lengkap.
		if partialSince.IsZero() {
			*partialSince = time.Now()
		}
		if time.Since(*partialSince) < 2*time.Minute {
			return -1, errPartial
		}
		p.Log.Warn("pemulihan memakai data sebagian setelah menunggu 2 menit")
	}
	dets := make([]detection.Detection, 0, len(items))
	for _, it := range items {
		var d detection.Detection
		if json.Unmarshal(it.Data, &d) == nil && d.Key != "" {
			dets = append(dets, d)
		}
	}
	p.Engine.Restore(dets, cp.MaxEventTime, cp.VirtualNow, cp.ActiveFires, cp.Stats)
	p.committed.Store(cp.Offset)
	p.Log.Info("keadaan satelit dipulihkan dari checkpoint", "offset", cp.Offset, "active_detections", len(dets), "active_fires", len(cp.ActiveFires))
	// Rekonsiliasi: hitung ulang dan samakan HazardEvent tersimpan dengan hasil hitungan.
	if err := p.flush(ctx, cp.Offset, nil); err != nil {
		return -1, err
	}
	return cp.Offset, nil
}

func toAgg(list []detection.Detection, final bool) []aggclient.Detection {
	out := make([]aggclient.Detection, 0, len(list))
	for _, d := range list {
		d.Raw = nil // atribut yang dipakai sudah terurai; payload asli tidak perlu disimpan ulang
		data, _ := json.Marshal(d)
		out = append(out, aggclient.Detection{Key: d.Key, Offset: d.Offset, Final: final, Data: data})
	}
	return out
}

// flush menghitung perubahan dan menyimpannya; diulang sampai berhasil atau ctx selesai.
func (p *Pipeline) flush(ctx context.Context, offset int64, received []time.Time) error {
	ch := p.Engine.Flush()
	// Tanda final dari batch sebelumnya, kecuali deteksi yang pada batch ini dicairkan
	// atau ditarik (keadaan terbarunya ada di PutDetections/DeleteDetections).
	touched := map[string]bool{}
	for _, d := range ch.PutDetections {
		touched[d.Key] = true
	}
	for _, k := range ch.DeleteDetections {
		touched[k] = true
	}
	var finals []detection.Detection
	for _, d := range p.pendingFinal {
		if !touched[d.Key] {
			finals = append(finals, d)
		}
	}
	b := aggclient.Batch{
		Offset:           offset,
		MaxEventTime:     p.Engine.MaxEventTime(),
		VirtualNow:       p.Engine.VirtualNow(),
		PutDetections:    append(toAgg(finals, true), toAgg(ch.PutDetections, false)...),
		DeleteDetections: ch.DeleteDetections,
		Late:             toAgg(ch.Late, false),
		UpsertFires:      ch.UpsertFires,
		DeleteFires:      ch.DeleteFires,
		ActiveFires:      p.Engine.ActiveFires(),
		Stats:            p.Engine.Stats(),
	}
	if p.LeaseToken != nil {
		b.LeaseToken = p.LeaseToken()
	}
	backoff := 500 * time.Millisecond
	for {
		err := p.Store.Apply(ctx, b)
		if err == nil {
			break
		}
		batchFail(ctx, 1)
		p.Log.Warn("batch satelit belum tersimpan, diulang", "offset", offset, "err", err, "backoff_ms", backoff.Milliseconds())
		if !sleepCtx(ctx, jitter(backoff)) {
			return ctx.Err()
		}
		backoff = min(backoff*2, 10*time.Second)
	}
	p.pendingFinal = ch.FinalDetections
	p.committed.Store(offset)
	p.maxEventUnix.Store(b.MaxEventTime.Unix())
	p.stats.Store(b.Stats)
	now := time.Now()
	for _, t := range received {
		ingestLat(ctx, now.Sub(t).Seconds())
	}
	firesC(ctx, int64(len(ch.UpsertFires)), "op", "upsert")
	firesC(ctx, int64(len(ch.DeleteFires)), "op", "delete")
	if len(ch.UpsertFires)+len(ch.DeleteFires)+len(ch.FinalizedFires)+len(ch.Late) > 0 {
		p.Log.Info("batch satelit tersimpan", "offset", offset, "events", len(received),
			"fires_upserted", len(ch.UpsertFires), "fires_deleted", len(ch.DeleteFires),
			"fires_finalized", len(ch.FinalizedFires), "late", len(ch.Late))
	}
	return nil
}

func (p *Pipeline) apply(ctx context.Context, e feed.Event) {
	outcome := "ignored"
	defer func() { eventsC(ctx, 1, "type", e.Type, "outcome", outcome) }()
	if vnow, ok := detection.VirtualNow(e.Data); ok {
		p.Engine.ObserveVirtualNow(vnow)
	}
	switch e.Type {
	case "detection", "update":
		d, err := detection.Parse(e.Data)
		if err != nil {
			outcome = "invalid"
			p.Log.Warn("kejadian feed tidak valid dilewati", "offset", e.ID, "type", e.Type, "err", err)
			return
		}
		d.Offset, d.ReceivedAt = e.ID, e.ReceivedAt
		if e.Type == "detection" {
			outcome = string(p.Engine.Add(d))
		} else {
			outcome = string(p.Engine.Update(d))
		}
	case "retract":
		key, err := detection.ParseKey(e.Data)
		if err != nil {
			outcome = "invalid"
			p.Log.Warn("retract tidak valid dilewati", "offset", e.ID, "err", err)
			return
		}
		outcome = string(p.Engine.Retract(key))
	case "heartbeat":
		outcome = "heartbeat"
	}
}

func classify(err error) (reason string, wait time.Duration) {
	var se *feed.StatusError
	switch {
	case errors.As(err, &se) && se.Code == http.StatusUnauthorized:
		return "unauthorized", time.Minute
	case errors.As(err, &se) && se.Code == http.StatusTooManyRequests:
		return "too_many_connections", 10 * time.Second
	case errors.As(err, &se) && se.Code == http.StatusServiceUnavailable:
		return "unavailable", 0
	case errors.Is(err, feed.ErrGap):
		return "gap", 0
	case errors.Is(err, feed.ErrHeartbeatTimeout):
		return "heartbeat_timeout", 0
	default:
		return "disconnected", 0
	}
}

func (p *Pipeline) readLoop(ctx context.Context, start int64, out chan<- feed.Event) {
	const minBackoff = 100 * time.Millisecond
	last := start
	backoff := minBackoff
	gapAt, gapCount := int64(-2), 0
	for ctx.Err() == nil {
		before := last
		var err error
		last, err = p.Feed.Stream(ctx, last, func(e feed.Event) error {
			p.unauthorized.Store(false)
			select {
			case out <- e:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if ctx.Err() != nil {
			return
		}
		reason, wait := classify(err)
		reconnects(ctx, 1, "reason", reason)
		if reason == "unauthorized" {
			p.unauthorized.Store(true)
			p.Log.Error("token Feed Satelit ditolak (401), periksa SATELLITE_TOKEN", "err", err)
		} else {
			p.Log.Warn("koneksi Feed Satelit terputus, menyambung ulang", "reason", reason, "last_offset", last, "err", err)
		}
		if reason == "gap" {
			gaps(ctx, 1)
			if last == gapAt {
				gapCount++
			} else {
				gapAt, gapCount = last, 1
			}
			if gapCount >= 3 && p.AllowGap != nil {
				p.Log.Error("lompatan offset yang sama berulang, offset yang hilang diterima apa adanya", "after", last)
				p.AllowGap()
			}
		}
		// Koneksi yang sempat membawa data baru berarti feed sehat: sambung ulang segera.
		// Backoff hanya membesar bila sambungan gagal berturut-turut tanpa kemajuan.
		if last > before {
			backoff = minBackoff
		}
		if wait == 0 {
			wait = jitter(backoff)
			backoff = min(backoff*2, 30*time.Second)
		}
		if !sleepCtx(ctx, wait) {
			return
		}
	}
}

func (p *Pipeline) sessionLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		if s, err := p.Feed.Session(ctx); err == nil {
			p.latest.Store(s.LatestOffset)
			if !s.VirtualNow.IsZero() {
				p.sessionVNow.Store(s.VirtualNow.Unix())
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Run berjalan sampai ctx selesai.
func (p *Pipeline) Run(ctx context.Context) error {
	p.registerMetrics()
	start, err := p.Recover(ctx)
	if err != nil {
		return err
	}
	p.ready.Store(true)
	events := make(chan feed.Event, 4096)
	go p.readLoop(ctx, start, events)
	go p.sessionLoop(ctx)

	offset := start
	var received []time.Time
	pending := 0
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	flushNow := func() error {
		if pending == 0 {
			return nil
		}
		batchSizeH(ctx, float64(pending))
		err := p.flush(ctx, offset, received)
		received, pending = nil, 0
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-events:
			p.apply(ctx, e)
			if e.ID >= 0 {
				offset = e.ID
			}
			if e.Type != "heartbeat" {
				received = append(received, e.ReceivedAt)
			}
			pending++
			if pending == 1 {
				timer.Reset(p.BatchWait)
			}
			if pending >= p.BatchMax {
				timer.Stop()
				if err := flushNow(); err != nil {
					return err
				}
			}
		case <-timer.C:
			if err := flushNow(); err != nil {
				return err
			}
		}
	}
}
