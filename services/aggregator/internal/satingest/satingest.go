package satingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/shardstore"
	"telemetry"
)

const (
	CollDetections = "satellite_detections"
	CollLate       = "satellite_late"
	CollCheckpoint = "ingest_checkpoints"
	CheckpointKey  = "satellite"
	maxBatchBytes  = 32 << 20
)

type Detection struct {
	Key    string          `json:"key"`
	Offset int64           `json:"offset"`
	Final  bool            `json:"final"`
	Data   json.RawMessage `json:"data"`
}

type Checkpoint struct {
	Offset       int64             `json:"offset"`
	MaxEventTime time.Time         `json:"max_event_time"`
	VirtualNow   time.Time         `json:"virtual_now"`
	ActiveFires  map[string]string `json:"active_fires"`
	Stats        map[string]int64  `json:"stats"`
	UpdatedAt    time.Time         `json:"updated_at"`
	FeedLatest   int64             `json:"feed_latest_offset,omitempty"`
	IngestP95    float64           `json:"ingest_p95_s,omitempty"`
}

type Batch struct {
	LeaseToken       string            `json:"lease_token,omitempty"`
	Offset           int64             `json:"offset"`
	MaxEventTime     time.Time         `json:"max_event_time"`
	VirtualNow       time.Time         `json:"virtual_now"`
	PutDetections    []Detection       `json:"put_detections"`
	DeleteDetections []string          `json:"delete_detections"`
	Late             []Detection       `json:"late"`
	UpsertFires      []hazard.Event    `json:"upsert_fires"`
	DeleteFires      []string          `json:"delete_fires"`
	ActiveFires      map[string]string `json:"active_fires"`
	Stats            map[string]int64  `json:"stats"`
	FeedLatest       int64             `json:"feed_latest_offset,omitempty"`
	IngestP95        float64           `json:"ingest_p95_s,omitempty"`
}

type Docs interface {
	Put(ctx context.Context, d shardstore.Doc) error
	Get(ctx context.Context, collection, key string) (*shardstore.Doc, error)
	Delete(ctx context.Context, collection, key string) error
	ScanAll(ctx context.Context, q shardstore.DocQuery) ([]shardstore.Doc, bool, error)
}

type Events interface {
	Save(ctx context.Context, ev hazard.Event) error
	Delete(ctx context.Context, hazardID string) error
}

type Publisher interface {
	Publish(ctx context.Context, event hazard.Event, correlationID string) error
}

type Service struct {
	docs   Docs
	events Events
	pub    Publisher
	log    *slog.Logger

	Fence func(token string) error
}

func New(docs Docs, events Events, pub Publisher, log *slog.Logger) *Service {
	return &Service{docs: docs, events: events, pub: pub, log: log}
}

var (
	batchesApplied = telemetry.Counter("satellite_batches_applied", "batch Feed Satelit yang berhasil diterapkan")
	firesWritten   = telemetry.Counter("satellite_fires_written", "HazardEvent WILDFIRE yang disimpan atau dihapus")
)

var ErrInvalid = errors.New("batch tidak valid")

func (s *Service) validate(b *Batch) error {
	for _, f := range b.UpsertFires {
		if f.Source != hazard.SourceSatelit || f.HazardType != hazard.TypeWildfire || !strings.HasPrefix(f.HazardID, "hz-") || f.SourceRefID == "" {
			return fmt.Errorf("%w: kebakaran %q tidak sesuai kontrak WILDFIRE/SATELIT", ErrInvalid, f.HazardID)
		}
	}
	for _, id := range b.DeleteFires {
		if !strings.HasPrefix(id, "hz-fire-") {
			return fmt.Errorf("%w: hanya HazardEvent kebakaran yang boleh dihapus (%q)", ErrInvalid, id)
		}
	}
	return nil
}

func parallel(ctx context.Context, n int, fn func(i int) error) error {
	const workers = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	idx := make(chan int)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				if err := fn(i); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	for i := 0; i < n && ctx.Err() == nil; i++ {
		idx <- i
	}
	close(idx)
	wg.Wait()
	if first == nil {
		first = ctx.Err()
	}
	return first
}

func offsetKey(o int64) string { return fmt.Sprintf("%019d", o) }

func (s *Service) Apply(ctx context.Context, b Batch, correlationID string) error {
	if s.Fence != nil {
		if err := s.Fence(b.LeaseToken); err != nil {
			return err
		}
	}
	if err := s.validate(&b); err != nil {
		return err
	}
	putDet := func(coll string, list []Detection) error {
		return parallel(ctx, len(list), func(i int) error {
			d := list[i]
			data, _ := json.Marshal(d)
			idx := map[string]string{"final": strconv.FormatBool(d.Final)}
			return s.docs.Put(ctx, shardstore.Doc{Collection: coll, Key: d.Key, SortKey: offsetKey(d.Offset), Index: idx, Data: data})
		})
	}
	if err := putDet(CollDetections, b.PutDetections); err != nil {
		return err
	}
	if err := putDet(CollLate, b.Late); err != nil {
		return err
	}
	if err := parallel(ctx, len(b.DeleteDetections), func(i int) error {
		return s.docs.Delete(ctx, CollDetections, b.DeleteDetections[i])
	}); err != nil {
		return err
	}
	if err := parallel(ctx, len(b.UpsertFires), func(i int) error {
		ev := b.UpsertFires[i]
		if err := s.events.Save(ctx, ev); err != nil {
			return err
		}
		firesWritten(ctx, 1, "op", "upsert")
		if s.pub != nil {
			return s.pub.Publish(ctx, ev, correlationID)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := parallel(ctx, len(b.DeleteFires), func(i int) error {
		if err := s.events.Delete(ctx, b.DeleteFires[i]); err != nil {
			return err
		}
		firesWritten(ctx, 1, "op", "delete")
		return nil
	}); err != nil {
		return err
	}
	cp := Checkpoint{Offset: b.Offset, MaxEventTime: b.MaxEventTime, VirtualNow: b.VirtualNow, ActiveFires: b.ActiveFires, Stats: b.Stats, UpdatedAt: time.Now().UTC(), FeedLatest: b.FeedLatest, IngestP95: b.IngestP95}
	data, _ := json.Marshal(cp)
	if err := s.docs.Put(ctx, shardstore.Doc{Collection: CollCheckpoint, Key: CheckpointKey, Data: data}); err != nil {
		return err
	}
	batchesApplied(ctx, 1)
	return nil
}

// Progress meringkas kemajuan pemrosesan aliran satelit untuk dashboard: seberapa tertinggal sistem dari feed (jumlah kejadian dan
// waktu virtual), p95 latensi masuk-hingga-tersimpan, dan keadaan mesin pengelompokan.
func (s *Service) Progress(ctx context.Context) (map[string]any, error) {
	cp, err := s.Checkpoint(ctx)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return map[string]any{"available": false, "reason": "belum ada checkpoint satelit"}, nil
	}
	lag := int64(0)
	if cp.FeedLatest > cp.Offset {
		lag = cp.FeedLatest - cp.Offset
	}
	timeLag := 0.0
	if !cp.VirtualNow.IsZero() && !cp.MaxEventTime.IsZero() && cp.VirtualNow.After(cp.MaxEventTime) {
		timeLag = cp.VirtualNow.Sub(cp.MaxEventTime).Seconds()
	}
	return map[string]any{
		"available": true, "offset": cp.Offset, "feed_latest_offset": cp.FeedLatest, "lag_events": lag,
		"event_time_lag_seconds": timeLag, "ingest_p95_seconds": cp.IngestP95, "active_fires": len(cp.ActiveFires),
		"updated_at": cp.UpdatedAt, "age_seconds": time.Since(cp.UpdatedAt).Seconds(),
		"late": cp.Stats["late"], "retracted": cp.Stats["retracted"], "duplicate": cp.Stats["duplicate"], "accepted": cp.Stats["accepted"],
	}, nil
}

func (s *Service) Checkpoint(ctx context.Context) (*Checkpoint, error) {
	doc, err := s.docs.Get(ctx, CollCheckpoint, CheckpointKey)
	if err != nil || doc == nil {
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(doc.Data, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

// ActiveDetections mengembalikan deteksi (aktif maupun yang sudah dibekukan) dengan
// offset <= maxOffset, untuk memulihkan keadaan satellite-ingestor setelah mati.
// Deteksi beku tetap dibutuhkan agar update/retract yang datang belakangan untuk
// kebakaran yang sudah selesai dikenali diperlakukan sama seperti tanpa gangguan.
func (s *Service) ActiveDetections(ctx context.Context, maxOffset int64) ([]Detection, bool, error) {
	docs, partial, err := s.docs.ScanAll(ctx, shardstore.DocQuery{Collection: CollDetections})
	if err != nil {
		return nil, false, err
	}
	out := make([]Detection, 0, len(docs))
	for _, d := range docs {
		var det Detection
		if json.Unmarshal(d.Data, &det) == nil && det.Offset <= maxOffset {
			out = append(out, det)
		}
	}
	return out, partial, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

func (s *Service) Register(mux *http.ServeMux, auth func(http.Handler) http.Handler, statsAuth func(http.Handler) http.Handler) {
	mux.Handle("POST /internal/ingest/satellite/batch", auth(http.HandlerFunc(s.handleBatch)))
	mux.Handle("GET /internal/ingest/satellite/checkpoint", auth(http.HandlerFunc(s.handleCheckpoint)))
	mux.Handle("GET /internal/ingest/satellite/detections", auth(http.HandlerFunc(s.handleDetections)))
	mux.Handle("GET /internal/satellite/stats", statsAuth(http.HandlerFunc(s.handleStats)))
}

// extendWriteDeadline memberi jawaban endpoint satelit waktu tulis lebih panjang daripada WriteTimeout server (15 detik).
// Menerapkan satu batch dan memulihkan deteksi aktif dapat memakan puluhan detik; tanpa ini server memutus koneksi di detik ke-15,
// pengirim menerima EOF dan mengulang batch yang sama tanpa henti walau batch sudah tersimpan (offset tidak pernah maju).
func extendWriteDeadline(w http.ResponseWriter, d time.Duration) {
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(d))
}

func (s *Service) handleBatch(w http.ResponseWriter, r *http.Request) {
	extendWriteDeadline(w, 90*time.Second)
	var b Batch
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBatchBytes))
	if err := dec.Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "batch tidak terbaca: "+err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	err := s.Apply(ctx, b, r.Header.Get("X-Correlation-ID"))
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"offset": b.Offset})
	case errors.Is(err, ErrInvalid):
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
	case errors.Is(err, ErrFenced):
		writeErr(w, http.StatusConflict, "fenced", err.Error())
	default:
		s.log.Warn("batch satelit gagal diterapkan, akan diulang oleh pengirim", "err", err, "offset", b.Offset)
		writeErr(w, http.StatusServiceUnavailable, "unavailable", err.Error())
	}
}

var ErrFenced = errors.New("token kepemimpinan usang")

func (s *Service) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	cp, err := s.Checkpoint(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", err.Error())
		return
	}
	if cp == nil {
		writeJSON(w, http.StatusOK, map[string]any{"found": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"found": true, "checkpoint": cp})
}

func (s *Service) handleDetections(w http.ResponseWriter, r *http.Request) {
	extendWriteDeadline(w, 90*time.Second)
	maxOffset, err := strconv.ParseInt(r.URL.Query().Get("max_offset"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "max_offset wajib bilangan bulat")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	dets, partial, err := s.ActiveDetections(ctx, maxOffset)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": dets, "partial": partial})
}

func (s *Service) handleStats(w http.ResponseWriter, r *http.Request) {
	cp, err := s.Checkpoint(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", err.Error())
		return
	}
	if cp == nil {
		writeJSON(w, http.StatusOK, map[string]any{"offset": -1, "stats": map[string]int64{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"offset": cp.Offset, "max_event_time": cp.MaxEventTime, "updated_at": cp.UpdatedAt,
		"active_fires": len(cp.ActiveFires), "stats": cp.Stats,
	})
}
