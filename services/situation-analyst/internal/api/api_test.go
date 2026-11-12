package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"situation-analyst/internal/engine"
	"situation-analyst/internal/window"
)

type fakeSource struct{ res *engine.Result }

func (f fakeSource) Latest() *engine.Result { return f.res }

func call(h http.Handler, path, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if key != "" {
		req.Header.Set("X-Internal-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAnalysisRequiresTheInternalKey(t *testing.T) {
	h := Routes(fakeSource{}, "rahasia", nil)
	if rec := call(h, "/analysis/latest", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("tanpa kunci: %d, want 401", rec.Code)
	}
	if rec := call(h, "/analysis/latest", "salah"); rec.Code != http.StatusForbidden {
		t.Fatalf("kunci salah: %d, want 403", rec.Code)
	}
	if rec := call(h, "/analysis/latest", "rahasia"); rec.Code != http.StatusOK {
		t.Fatalf("kunci benar: %d, want 200", rec.Code)
	}
}

func TestBeforeTheFirstAnalysisTheAnswerSaysUnavailable(t *testing.T) {
	rec := call(Routes(fakeSource{}, "k", nil), "/analysis/latest", "k")
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["available"] != false {
		t.Fatalf("body = %s err=%v", rec.Body.String(), err)
	}
}

func TestLatestResultIsReturnedAsJSON(t *testing.T) {
	res := &engine.Result{Available: true, GeneratedAt: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC), Source: "template",
		Summary: "Ringkasan.", Window: window.Snapshot{WindowMinutes: 15, Total: 3, ByCategory: map[string]int{"WILDFIRE": 3}},
		LLM: engine.LLMStatus{Status: "disabled"}, Disclaimer: engine.Disclaimer}
	rec := call(Routes(fakeSource{res}, "k", nil), "/analysis/latest", "k")
	var got engine.Result
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("bukan JSON: %v", err)
	}
	if got.Source != "template" || got.Summary != "Ringkasan." || got.Window.Total != 3 || got.LLM.Status != "disabled" || got.Disclaimer == "" {
		t.Fatalf("got = %+v", got)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("hasil analisis tidak boleh di-cache")
	}
}

func TestHealthStaysOKEvenWhenTheBrokerIsDown(t *testing.T) {
	var connected atomic.Bool
	h := Routes(fakeSource{}, "k", &connected)
	rec := call(h, "/health", "")
	if rec.Code != http.StatusOK || !json.Valid(rec.Body.Bytes()) {
		t.Fatalf("health = %d %s", rec.Code, rec.Body.String())
	}
	if want := `"degraded"`; !strings.Contains(rec.Body.String(), want) {
		t.Fatalf("body = %s, want %s", rec.Body.String(), want)
	}
	connected.Store(true)
	if rec := call(h, "/health", ""); !strings.Contains(rec.Body.String(), `"ok"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}
