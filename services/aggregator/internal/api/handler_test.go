package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/httpx"
	"aggregator/internal/poller"
	"aggregator/internal/store"
)

type fakeReader struct {
	events    []hazard.Event
	err       error
	pingErr   error
	gotLimit  int64
	gotFilter store.Filter
	calls     int
}

func (f *fakeReader) GetHazardEvents(limit int64, filter store.Filter) ([]hazard.Event, error) {
	f.calls++
	f.gotLimit, f.gotFilter = limit, filter
	return f.events, f.err
}

func (f *fakeReader) Ping(context.Context) error { return f.pingErr }

type fakeStatus struct{ status poller.PVMBGStatus }

func (f fakeStatus) PVMBGStatus() poller.PVMBGStatus { return f.status }

type fakeBroker struct{ connected bool }

func (f fakeBroker) Connected() bool { return f.connected }

func newHandler(reader *fakeReader, status poller.PVMBGStatus, brokerUp bool) *Handler {
	return newHandlerWithLog(reader, status, brokerUp, slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

func newHandlerWithLog(reader *fakeReader, status poller.PVMBGStatus, brokerUp bool, log *slog.Logger) *Handler {
	return NewHandler(reader, fakeStatus{status: status}, fakeBroker{connected: brokerUp}, log)
}

func get(h http.HandlerFunc, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func TestGetHazardEventsDefaultsAndFilters(t *testing.T) {
	reader := &fakeReader{}
	h := newHandler(reader, poller.PVMBGStatus{SourceStatus: poller.SourceHealthy}, true)

	rec := get(h.GetHazardEvents, "/hazard-events?limit=20&hazard_type=VOLCANIC&severity=AWAS&since=2026-10-08T01:02:03Z")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	wantSince, _ := time.Parse(time.RFC3339, "2026-10-08T01:02:03Z")
	if reader.gotLimit != 20 || reader.gotFilter.HazardType != "VOLCANIC" || reader.gotFilter.Severity != "AWAS" ||
		reader.gotFilter.Since == nil || !reader.gotFilter.Since.Equal(wantSince) {
		t.Fatalf("limit=%d filter=%#v", reader.gotLimit, reader.gotFilter)
	}

	get(h.GetHazardEvents, "/hazard-events")
	if reader.gotLimit != 50 || reader.gotFilter != (store.Filter{}) {
		t.Fatalf("defaults: limit=%d filter=%#v, want limit 50 and no filter", reader.gotLimit, reader.gotFilter)
	}
}

func TestGetHazardEventsLimitIsBounded(t *testing.T) {
	tests := []struct {
		query string
		want  int64
	}{
		{query: "limit=100000", want: maxLimit},
		{query: "limit=500", want: 500},
		{query: "limit=abc", want: 50},
		{query: "limit=0", want: 50},
		{query: "limit=-4", want: 50},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			reader := &fakeReader{}
			h := newHandler(reader, poller.PVMBGStatus{}, true)
			get(h.GetHazardEvents, "/hazard-events?"+tt.query)
			if reader.gotLimit != tt.want {
				t.Fatalf("limit = %d, want %d", reader.gotLimit, tt.want)
			}
		})
	}
}

func TestGetHazardEventsRejectsInvalidFilters(t *testing.T) {
	for _, query := range []string{"hazard_type=FLOOD", "severity=KRITIS", "since=yesterday", "hazard_type=seismic"} {
		t.Run(query, func(t *testing.T) {
			reader := &fakeReader{}
			h := newHandler(reader, poller.PVMBGStatus{}, true)

			rec := get(h.GetHazardEvents, "/hazard-events?"+query)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			if reader.calls != 0 {
				t.Fatal("an invalid request must not reach the store")
			}
		})
	}
}

func TestGetHazardEventsReturnsServerErrorWhenStoreFails(t *testing.T) {
	h := newHandler(&fakeReader{err: errors.New("mongo down")}, poller.PVMBGStatus{}, true)

	rec := get(h.GetHazardEvents, "/hazard-events")
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "mongo down") {
		t.Fatalf("status=%d body=%q; internal errors must not leak", rec.Code, rec.Body.String())
	}
}

func TestGetHazardEventsEnvelopeCarriesFreshnessMetadata(t *testing.T) {
	staleSince := time.Now().UTC().Add(-time.Minute)
	reader := &fakeReader{events: []hazard.Event{{
		HazardID: "hz-1", Source: hazard.SourcePVMBG, HazardType: hazard.TypeVolcanic, Severity: hazard.SeverityAwas,
		Attributes: map[string]any{"confidence_level": 0.9},
	}}}
	h := newHandler(reader, poller.PVMBGStatus{SourceStatus: poller.SourceDegraded, StaleSince: &staleSince, Stale: true}, true)

	rec := get(h.GetHazardEvents, "/hazard-events")
	var body struct {
		Data []map[string]any `json:"data"`
		Meta struct {
			Sources struct {
				PVMBG poller.PVMBGStatus `json:"pvmbg"`
			} `json:"sources"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not the expected envelope: %v\n%s", err, rec.Body.String())
	}
	if len(body.Data) != 1 || body.Data[0]["hazard_id"] != "hz-1" {
		t.Fatalf("data = %#v", body.Data)
	}
	attrs, _ := body.Data[0]["attributes"].(map[string]any)
	if attrs["confidence_level"] != 0.9 {
		t.Fatalf("attributes = %#v", attrs)
	}
	pvmbg := body.Meta.Sources.PVMBG
	if pvmbg.SourceStatus != poller.SourceDegraded || !pvmbg.Stale || pvmbg.StaleSince == nil {
		t.Fatalf("meta must tell the client that volcanic data is stale: %#v", pvmbg)
	}
}

func TestGetHazardEventsEmptyResultIsAnEmptyArray(t *testing.T) {
	h := newHandler(&fakeReader{}, poller.PVMBGStatus{}, true)
	rec := get(h.GetHazardEvents, "/hazard-events")
	if !strings.Contains(rec.Body.String(), `"data":[]`) {
		t.Fatalf("an empty result must serialise as [], got %s", rec.Body.String())
	}
}

func TestGetPVMBGStatus(t *testing.T) {
	now := time.Now().UTC()
	h := newHandler(&fakeReader{}, poller.PVMBGStatus{SourceStatus: poller.SourceHealthy, LastSuccessAt: &now}, true)

	rec := get(h.GetPVMBGStatus, "/pvmbg-status")
	var got poller.PVMBGStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got.SourceStatus != poller.SourceHealthy || got.LastSuccessAt == nil {
		t.Fatalf("status = %#v err=%v", got, err)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content type = %q", rec.Header().Get("Content-Type"))
	}
}

func TestHealthReflectsOnlyTheCanonicalStoreAsFatal(t *testing.T) {
	tests := []struct {
		name       string
		pingErr    error
		brokerUp   bool
		wantCode   int
		wantStatus string
	}{
		{name: "everything up", brokerUp: true, wantCode: http.StatusOK, wantStatus: "ok"},
		{name: "broker down only degrades", brokerUp: false, wantCode: http.StatusOK, wantStatus: "degraded"},
		{name: "mongo down is unavailable", pingErr: errors.New("down"), brokerUp: true, wantCode: http.StatusServiceUnavailable, wantStatus: "unavailable"},
		{name: "mongo down wins over broker down", pingErr: errors.New("down"), brokerUp: false, wantCode: http.StatusServiceUnavailable, wantStatus: "unavailable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHandler(&fakeReader{pingErr: tt.pingErr}, poller.PVMBGStatus{}, tt.brokerUp)
			rec := get(h.health, "/health")

			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("health body: %v", err)
			}
			if rec.Code != tt.wantCode || body["status"] != tt.wantStatus {
				t.Fatalf("code=%d body=%v, want %d/%s", rec.Code, body, tt.wantCode, tt.wantStatus)
			}
			if body["mongo"] != (tt.pingErr == nil) || body["rabbitmq"] != tt.brokerUp {
				t.Fatalf("component flags wrong: %v", body)
			}
		})
	}
}

func TestRoutesProtectDataEndpointsButNotHealth(t *testing.T) {
	const key = "internal-key-for-test"
	h := newHandler(&fakeReader{}, poller.PVMBGStatus{}, true)
	routes := h.Routes(key, httpx.Observe(slog.New(slog.NewJSONHandler(io.Discard, nil))))

	tests := []struct {
		name   string
		path   string
		header string
		want   int
	}{
		{name: "health is open", path: "/health", want: http.StatusOK},
		{name: "events without key", path: "/hazard-events", want: http.StatusUnauthorized},
		{name: "events with wrong key", path: "/hazard-events", header: "nope", want: http.StatusForbidden},
		{name: "events with key", path: "/hazard-events", header: key, want: http.StatusOK},
		{name: "status without key", path: "/pvmbg-status", want: http.StatusUnauthorized},
		{name: "status with key", path: "/pvmbg-status", header: key, want: http.StatusOK},
		{name: "unknown route", path: "/admin", header: key, want: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.header != "" {
				req.Header.Set("X-Internal-Key", tt.header)
			}
			rec := httptest.NewRecorder()
			routes.ServeHTTP(rec, req)

			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
			if rec.Header().Get("X-Correlation-ID") == "" {
				t.Fatal("every response must carry a correlation id")
			}
		})
	}
}

func TestMongoCallsAreLoggedWithLatencyAndCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	reader := &fakeReader{}
	h := newHandlerWithLog(reader, poller.PVMBGStatus{}, true, log)
	routes := h.Routes("internal-key-for-test", httpx.Observe(log))

	send := func(path string, withKey bool) {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("X-Correlation-ID", "trace-77")
		if withKey {
			req.Header.Set("X-Internal-Key", "internal-key-for-test")
		}
		routes.ServeHTTP(httptest.NewRecorder(), req)
	}
	send("/hazard-events", true)
	send("/health", false)

	calls := map[string]map[string]any{}
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if rec["msg"] == "mongodb call" {
			calls[rec["operation"].(string)] = rec
		}
	}
	for _, operation := range []string{"find_hazard_events", "ping"} {
		rec, ok := calls[operation]
		if !ok {
			t.Fatalf("no latency log for %s, got %v", operation, calls)
		}
		if d, ok := rec["duration_ms"].(float64); !ok || d < 0 {
			t.Fatalf("%s: duration_ms missing or invalid: %v", operation, rec["duration_ms"])
		}
		if rec["target"] != "mongodb" || rec["success"] != true || rec["correlation_id"] != "trace-77" {
			t.Fatalf("%s: unexpected record %v", operation, rec)
		}
	}
}

func TestFailedMongoCallsAreLoggedAsUnsuccessful(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	reader := &fakeReader{err: errors.New("mongo down"), pingErr: errors.New("mongo down")}
	h := newHandlerWithLog(reader, poller.PVMBGStatus{}, true, log)

	get(h.GetHazardEvents, "/hazard-events")
	get(h.health, "/health")

	failures := 0
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if rec["msg"] == "mongodb call failed" {
			failures++
			if rec["success"] != false || rec["level"] != "WARN" || rec["err"] == nil || rec["duration_ms"] == nil {
				t.Fatalf("unexpected failure record: %v", rec)
			}
		}
	}
	if failures != 2 {
		t.Fatalf("expected a failure log for both the query and the ping, got %d", failures)
	}
}

func TestNewHandlerFallsBackToTheDefaultLogger(t *testing.T) {
	h := NewHandler(&fakeReader{}, fakeStatus{}, fakeBroker{connected: true}, nil)
	if h.log == nil {
		t.Fatal("a nil logger must be replaced so the handler never panics while logging")
	}
	get(h.GetHazardEvents, "/hazard-events")
}
