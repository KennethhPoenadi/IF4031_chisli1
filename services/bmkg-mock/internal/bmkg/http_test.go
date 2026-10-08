package bmkg

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testAPIKey = "bmkg-key-for-tests"

type harness struct {
	handler http.Handler
	logs    *bytes.Buffer
	slept   []time.Duration
	store   *Store
}

func newHarness() *harness {
	h := &harness{logs: &bytes.Buffer{}, store: NewStore(20, 4031, fixedClock)}
	srv := NewServer(Config{APIKey: testAPIKey, Delay: 100 * time.Millisecond}, h.store, h.logs)
	srv.sleep = func(d time.Duration) { h.slept = append(h.slept, d) }
	h.handler = srv.Handler()
	return h
}

func (h *harness) do(method, target string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

func TestHealthNeedsNoCredentials(t *testing.T) {
	h := newHarness()
	rec := h.do(http.MethodGet, "/health", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"bmkg-mock"`) {
		t.Fatalf("health: %d %s", rec.Code, rec.Body.String())
	}
	if len(h.slept) != 0 {
		t.Fatal("health must answer immediately")
	}
}

func TestEndpointsRejectMissingAndForeignCredentials(t *testing.T) {
	h := newHarness()

	tests := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{name: "no credential", want: http.StatusUnauthorized},
		{name: "wrong BMKG key", headers: map[string]string{"X-BMKG-Key": "not-the-key"}, want: http.StatusForbidden},
		{name: "a PVMBG-style bearer token is not accepted", headers: map[string]string{"Authorization": "Bearer pvmbg-token"}, want: http.StatusUnauthorized},
		{name: "the BMKG key sent as a bearer token is not accepted", headers: map[string]string{"Authorization": "Bearer " + testAPIKey}, want: http.StatusUnauthorized},
		{name: "key with different case", headers: map[string]string{"X-BMKG-Key": strings.ToUpper(testAPIKey)}, want: http.StatusForbidden},
	}

	for _, path := range []string{"/seismic-events", "/tsunami-warnings"} {
		for _, tt := range tests {
			t.Run(path+" "+tt.name, func(t *testing.T) {
				rec := h.do(http.MethodGet, path, tt.headers)
				if rec.Code != tt.want {
					t.Fatalf("status = %d, want %d", rec.Code, tt.want)
				}
				if strings.Contains(rec.Body.String(), "bmkg-000001") {
					t.Fatal("data leaked to an unauthenticated caller")
				}
			})
		}
	}
	if len(h.slept) != 0 {
		t.Fatal("rejected requests must not incur the artificial delay")
	}
}

func TestSeismicEventsWithValidKeyAreServedWithTheFixedDelay(t *testing.T) {
	h := newHarness()

	rec := h.do(http.MethodGet, "/seismic-events", map[string]string{"X-BMKG-Key": testAPIKey})
	var events []SeismicEvent
	if err := json.Unmarshal(rec.Body.Bytes(), &events); err != nil || rec.Code != http.StatusOK || len(events) != 20 {
		t.Fatalf("status=%d events=%d err=%v", rec.Code, len(events), err)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content type = %q", rec.Header().Get("Content-Type"))
	}
	if len(h.slept) != 1 || h.slept[0] != 100*time.Millisecond {
		t.Fatalf("delays = %v, want exactly one fixed 100ms delay", h.slept)
	}
	for _, field := range []string{"event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon", "region_name", "occurred_at", "potential_tsunami"} {
		if !strings.Contains(rec.Body.String(), `"`+field+`"`) {
			t.Fatalf("contract field %q missing from the response", field)
		}
	}
}

func TestTsunamiWarningsContractFields(t *testing.T) {
	h := newHarness()

	rec := h.do(http.MethodGet, "/tsunami-warnings", map[string]string{"X-BMKG-Key": testAPIKey})
	var warnings []TsunamiWarning
	if err := json.Unmarshal(rec.Body.Bytes(), &warnings); err != nil || len(warnings) != 5 {
		t.Fatalf("warnings=%d err=%v", len(warnings), err)
	}
	for _, field := range []string{"warning_id", "related_event_id", "threat_level", "affected_zones", "estimated_arrival"} {
		if !strings.Contains(rec.Body.String(), `"`+field+`"`) {
			t.Fatalf("contract field %q missing", field)
		}
	}
	if strings.Contains(rec.Body.String(), "created") {
		t.Fatal("the internal creation timestamp must not leak into the contract")
	}
}

func TestSinceFiltersAndIsValidated(t *testing.T) {
	h := newHarness()
	key := map[string]string{"X-BMKG-Key": testAPIKey}
	tenth := h.store.EventsSince(nil)[9].OccurredAt

	rec := h.do(http.MethodGet, "/seismic-events?since="+tenth.Format(time.RFC3339Nano), key)
	var events []SeismicEvent
	json.Unmarshal(rec.Body.Bytes(), &events)
	if rec.Code != http.StatusOK || len(events) != 10 {
		t.Fatalf("since filter: %d events=%d", rec.Code, len(events))
	}

	rec = h.do(http.MethodGet, "/tsunami-warnings?since=2000-01-01T00:00:00Z", key)
	var warnings []TsunamiWarning
	json.Unmarshal(rec.Body.Bytes(), &warnings)
	if len(warnings) != 5 {
		t.Fatalf("warnings = %d", len(warnings))
	}

	for _, path := range []string{"/seismic-events", "/tsunami-warnings"} {
		before := len(h.slept)
		rec = h.do(http.MethodGet, path+"?since=yesterday", key)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "RFC3339") {
			t.Fatalf("%s: invalid since: %d %s", path, rec.Code, rec.Body.String())
		}
		if len(h.slept) != before {
			t.Fatal("a rejected request must not wait")
		}
	}
}

func TestEmptyResultIsAnEmptyJSONArray(t *testing.T) {
	h := newHarness()
	rec := h.do(http.MethodGet, "/seismic-events?since=2999-01-01T00:00:00Z", map[string]string{"X-BMKG-Key": testAPIKey})
	if strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("body = %q, want []", rec.Body.String())
	}
}

func TestOnlyGETIsAllowed(t *testing.T) {
	h := newHarness()
	for _, path := range []string{"/health", "/seismic-events", "/tsunami-warnings"} {
		if rec := h.do(http.MethodPost, path, map[string]string{"X-BMKG-Key": testAPIKey}); rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("POST %s = %d, want 405 (the mock is passive and read-only)", path, rec.Code)
		}
	}
	if rec := h.do(http.MethodGet, "/admin/outage", map[string]string{"X-BMKG-Key": testAPIKey}); rec.Code != http.StatusNotFound {
		t.Fatalf("BMKG has no admin endpoints, got %d", rec.Code)
	}
}

func TestRequestsAreLoggedWithTheCorrelationID(t *testing.T) {
	h := newHarness()

	rec := h.do(http.MethodGet, "/seismic-events", map[string]string{"X-BMKG-Key": testAPIKey, "X-Correlation-ID": "from-aggregator"})
	if rec.Header().Get("X-Correlation-ID") != "from-aggregator" {
		t.Fatalf("correlation id header = %q", rec.Header().Get("X-Correlation-ID"))
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(h.logs.Bytes()), &record); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, h.logs.String())
	}
	if record["correlation_id"] != "from-aggregator" || record["service"] != "bmkg-mock" || record["path"] != "/seismic-events" ||
		record["status"] != float64(200) || record["method"] != "GET" || record["duration_ms"] == nil || record["timestamp"] == nil {
		t.Fatalf("log record = %v", record)
	}
	if strings.Contains(h.logs.String(), testAPIKey) {
		t.Fatal("the API key must never be logged")
	}

	rec = h.do(http.MethodGet, "/health", nil)
	id := rec.Header().Get("X-Correlation-ID")
	if _, err := hex.DecodeString(id); err != nil || len(id) != 32 {
		t.Fatalf("generated correlation id = %q", id)
	}
}

func TestNewServerWithoutALogWriterStillWorks(t *testing.T) {
	srv := NewServer(Config{APIKey: testAPIKey, Delay: time.Millisecond}, NewStore(20, 1, fixedClock), nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}
