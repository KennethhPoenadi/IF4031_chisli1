package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"satellite-ingestor/internal/aggclient"
	"satellite-ingestor/internal/feed"
	"satellite-ingestor/internal/fire"
	"satellite-ingestor/internal/province"
)

// memStore meniru endpoint ingest Aggregator: deteksi, kebakaran, lalu checkpoint.
type memStore struct {
	mu     sync.Mutex
	dets   map[string]aggclient.Detection
	fires  map[string]string
	cp     *aggclient.Checkpoint
	failAt int // gagal (crash) setelah menulis kebakaran pada batch ke-N
	calls  int
}

var errCrash = errors.New("crash sebelum checkpoint")

func newMem() *memStore {
	return &memStore{dets: map[string]aggclient.Detection{}, fires: map[string]string{}}
}

func (m *memStore) Apply(_ context.Context, b aggclient.Batch) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	for _, d := range b.PutDetections {
		m.dets[d.Key] = d
	}
	for _, k := range b.DeleteDetections {
		delete(m.dets, k)
	}
	for _, f := range b.UpsertFires {
		f.IngestedAt = time.Time{}
		j, _ := json.Marshal(f)
		m.fires[f.HazardID] = string(j)
	}
	for _, id := range b.DeleteFires {
		delete(m.fires, id)
	}
	if m.calls == m.failAt {
		return errCrash
	}
	m.cp = &aggclient.Checkpoint{Offset: b.Offset, MaxEventTime: b.MaxEventTime, VirtualNow: b.VirtualNow, ActiveFires: b.ActiveFires, Stats: b.Stats}
	return nil
}

func (m *memStore) Checkpoint(context.Context) (*aggclient.Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cp == nil {
		return nil, nil
	}
	cp := *m.cp
	return &cp, nil
}

func (m *memStore) ActiveDetections(_ context.Context, maxOffset int64) ([]aggclient.Detection, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []aggclient.Detection
	for _, d := range m.dets {
		if d.Offset <= maxOffset {
			out = append(out, d)
		}
	}
	return out, false, nil
}

// memFeed meniru Feed Satelit: aliran berofset, terputus setiap disconnectEvery kejadian.
type memFeed struct {
	events          []feed.Event
	disconnectEvery int
	stopAt          int64 // berhenti total (simulasi pod mati) setelah offset ini
}

func (f *memFeed) Stream(ctx context.Context, last int64, handle func(feed.Event) error) (int64, error) {
	sent := 0
	for _, e := range f.events {
		if e.ID <= last {
			continue
		}
		if f.stopAt >= 0 && e.ID > f.stopAt {
			<-ctx.Done()
			return last, ctx.Err()
		}
		e.ReceivedAt = time.Now()
		if err := handle(e); err != nil {
			return last, err
		}
		last = e.ID
		sent++
		if f.disconnectEvery > 0 && sent >= f.disconnectEvery {
			return last, errors.New("putus")
		}
	}
	<-ctx.Done()
	return last, ctx.Err()
}

func (f *memFeed) Session(context.Context) (feed.Session, error) {
	return feed.Session{LatestOffset: int64(len(f.events) - 1)}, nil
}

func stream(seed int64) []feed.Event {
	rng := rand.New(rand.NewSource(seed))
	base := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	var out []feed.Event
	var keys []string
	for i := 0; i < 600; i++ {
		id := int64(len(out))
		if rng.Intn(15) == 0 && len(keys) > 0 {
			k := keys[rng.Intn(len(keys))]
			out = append(out, feed.Event{ID: id, Type: "retract", Data: []byte(fmt.Sprintf(`{"key":%q}`, k))})
			continue
		}
		if rng.Intn(10) == 0 {
			vnow := base.Add(time.Duration(i*2) * time.Minute)
			out = append(out, feed.Event{ID: id, Type: "heartbeat", Data: []byte(fmt.Sprintf(`{"virtual_now":%q}`, vnow.Format(time.RFC3339)))})
			continue
		}
		c := rng.Intn(30)
		at := base.Add(time.Duration(i*2-rng.Intn(90)) * time.Minute)
		lat := -3 + float64(c%6)*0.3 + rng.Float64()*0.012
		lon := 110 + float64(c/6)*0.3 + rng.Float64()*0.012
		sat := []string{"N20", "N21", "SNPP"}[rng.Intn(3)]
		conf := []string{"l", "n", "h"}[rng.Intn(3)]
		data := fmt.Sprintf(`{"satellite":%q,"acq_date":%q,"acq_time":%d,"latitude":%.5f,"longitude":%.5f,"confidence":%q,"frp":%.2f}`,
			sat, at.Format("2006-01-02"), at.Hour()*100+at.Minute(), lat, lon, conf, rng.Float64()*20)
		keys = append(keys, fmt.Sprintf("%s|%s|%d|%.5f|%.5f", sat, at.Format("2006-01-02"), at.Hour()*100+at.Minute(), lat, lon))
		out = append(out, feed.Event{ID: id, Type: "detection", Data: []byte(data)})
	}
	return out
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newPipeline(src Source, st Store) *Pipeline {
	return &Pipeline{Feed: src, Store: st, Engine: fire.New(120*time.Minute, province.Builtin()),
		BatchMax: 37, BatchWait: 5 * time.Millisecond, Log: quiet()}
}

// runUntil menjalankan pipeline sampai offset tertentu tersimpan atau terjadi crash.
func runUntil(t *testing.T, p *Pipeline, st *memStore, want int64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = p.Run(ctx); close(done) }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		st.mu.Lock()
		crashed := st.failAt > 0 && st.calls >= st.failAt
		st.mu.Unlock()
		if p.Committed() >= want || crashed {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done
	t.Logf("run selesai: committed=%d ingin=%d apply=%d", p.Committed(), want, st.calls)
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestPipelineCrashAndReconnectGiveIdenticalResult(t *testing.T) {
	for _, seed := range []int64{7, 11, 23, 42} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { crashScenario(t, seed) })
	}
}

func crashScenario(t *testing.T, seed int64) {
	events := stream(seed)
	last := int64(len(events) - 1)

	ref := newMem()
	runUntil(t, newPipeline(&memFeed{events: events, stopAt: -1}, ref), ref, last)
	if len(ref.fires) == 0 {
		t.Fatal("aliran uji harus menghasilkan kebakaran")
	}

	// Gangguan: koneksi putus setiap 23 kejadian, proses mati dua kali (sekali di tengah
	// penulisan batch, sekali karena pod dihentikan), lalu pulih dari checkpoint.
	st := newMem()
	st.failAt = 4
	runUntil(t, newPipeline(&memFeed{events: events, disconnectEvery: 23, stopAt: -1}, st), st, last)
	st.failAt = 0
	runUntil(t, newPipeline(&memFeed{events: events, disconnectEvery: 23, stopAt: 300}, st), st, 300)
	runUntil(t, newPipeline(&memFeed{events: events, disconnectEvery: 23, stopAt: -1}, st), st, last)

	if !reflect.DeepEqual(sortedKeys(st.fires), sortedKeys(ref.fires)) {
		t.Fatalf("kebakaran berbeda\nref %v\ngot %v", sortedKeys(ref.fires), sortedKeys(st.fires))
	}
	for id, v := range ref.fires {
		if st.fires[id] != v {
			t.Fatalf("isi %s berbeda:\nref %s\ngot %s", id, v, st.fires[id])
		}
	}
}
