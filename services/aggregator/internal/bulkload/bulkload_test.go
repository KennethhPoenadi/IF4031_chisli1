package bulkload

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"aggregator/internal/hazard"
)

type memStore struct {
	mu   sync.Mutex
	byID map[string]hazard.Event
	fail map[string]bool
}

func (m *memStore) Save(_ context.Context, ev hazard.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail[ev.HazardID] {
		return errors.New("tidak terjangkau")
	}
	if m.byID == nil {
		m.byID = map[string]hazard.Event{}
	}
	m.byID[ev.HazardID] = ev
	return nil
}

func quake(id string) map[string]any {
	return map[string]any{
		"event_id": id, "magnitude": 5.1, "depth_km": 10.0, "epicenter_lat": -0.9, "epicenter_lon": 100.3,
		"region_name": "Sumatera Barat", "occurred_at": "2026-11-01T02:03:04Z", "potential_tsunami": false,
	}
}

func TestLoadStoresValidRejectsInvalidWithReasonAndIsIdempotent(t *testing.T) {
	st := &memStore{}
	h := &Handler{Store: st}
	bad := quake("q-bad")
	delete(bad, "magnitude")
	batch := []map[string]any{quake("q-1"), bad, quake("q-2"), quake("q-1")}

	res := h.Load(context.Background(), batch)
	if res.Received != 4 || res.Stored != 3 || res.Rejected != 1 || res.Failed != 0 {
		t.Fatalf("res = %+v, want 4 diterima, 3 tersimpan (satu duplikat), 1 ditolak", res)
	}
	if len(res.Rejections) != 1 || res.Rejections[0].Index != 1 || res.Rejections[0].Reason == "" {
		t.Fatalf("rejections = %+v, want indeks 1 beserta alasan", res.Rejections)
	}
	if len(st.byID) != 2 {
		t.Fatalf("unik tersimpan = %d, want 2 (q-1 diulang tidak menggandakan)", len(st.byID))
	}
	if ev := st.byID["hz-q-1"]; ev.Source != hazard.SourceBMKG || ev.HazardType != hazard.TypeSeismic {
		t.Fatalf("event = %+v, want BMKG/SEISMIC", ev)
	}
}

func TestLoadCountsStorageFailuresSoTheSenderCanRetry(t *testing.T) {
	st := &memStore{fail: map[string]bool{"hz-q-2": true}}
	res := (&Handler{Store: st}).Load(context.Background(), []map[string]any{quake("q-1"), quake("q-2")})
	if res.Stored != 1 || res.Failed != 1 {
		t.Fatalf("res = %+v, want 1 tersimpan dan 1 gagal", res)
	}
}

func TestHandlerStatusCodes(t *testing.T) {
	pass := func(n http.Handler) http.Handler { return n }
	mux := http.NewServeMux()
	(&Handler{Store: &memStore{}}).Register(mux, pass)

	post := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/ingest/events", strings.NewReader(body)))
		return rec
	}
	if rec := post(`{"events":[]}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("kosong code = %d, want 400", rec.Code)
	}
	if rec := post(`bukan json`); rec.Code != http.StatusBadRequest {
		t.Fatalf("rusak code = %d, want 400", rec.Code)
	}
	if rec := post(`{"events":[{"event_id":"x"}]}`); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"rejected":1`) {
		t.Fatalf("tidak valid code = %d body=%s, want 200 dengan rejected 1", rec.Code, rec.Body.String())
	}

	failing := http.NewServeMux()
	(&Handler{Store: &memStore{fail: map[string]bool{"hz-q-1": true}}}).Register(failing, pass)
	rec := httptest.NewRecorder()
	failing.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/ingest/events",
		strings.NewReader(`{"events":[{"event_id":"q-1","magnitude":5.1,"depth_km":10,"epicenter_lat":-1,"epicenter_lon":100,"region_name":"A","occurred_at":"2026-11-01T02:03:04Z","potential_tsunami":false}]}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("gagal simpan code = %d, want 503", rec.Code)
	}
}
