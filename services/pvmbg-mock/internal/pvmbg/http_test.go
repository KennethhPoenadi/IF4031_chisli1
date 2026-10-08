package pvmbg

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

const testToken = "pvmbg-token-for-tests"

type harness struct {
	handler http.Handler
	logs    *bytes.Buffer
	slept   []time.Duration
	store   *Store
}

func newHarness() *harness {
	h := &harness{logs: &bytes.Buffer{}, store: NewStore(20, 4031, fixedClock)}
	srv := NewServer(Config{Token: testToken, DelayMin: 500 * time.Millisecond, DelayMax: 3 * time.Second}, h.store, h.logs)
	srv.sleep = func(d time.Duration) { h.slept = append(h.slept, d) }
	h.handler = srv.Handler()
	return h
}

func bearer() map[string]string { return map[string]string{"Authorization": "Bearer " + testToken} }

func (h *harness) do(method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

func (h *harness) reports(t *testing.T) []map[string]any {
	t.Helper()
	rec := h.do(http.MethodGet, "/volcanic-reports", "", bearer())
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /volcanic-reports = %d %s", rec.Code, rec.Body.String())
	}
	var out []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestEndpointsRejectMissingAndForeignCredentials(t *testing.T) {
	h := newHarness()

	tests := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{name: "no credential", want: http.StatusUnauthorized},
		{name: "wrong token", headers: map[string]string{"Authorization": "Bearer nope"}, want: http.StatusForbidden},
		{name: "a BMKG-style key header is not accepted", headers: map[string]string{"X-BMKG-Key": "bmkg-key"}, want: http.StatusUnauthorized},
		{name: "the token without the Bearer scheme", headers: map[string]string{"Authorization": testToken}, want: http.StatusForbidden},
		{name: "basic scheme", headers: map[string]string{"Authorization": "Basic " + testToken}, want: http.StatusForbidden},
	}
	endpoints := []struct{ method, path, body string }{
		{http.MethodGet, "/volcanic-reports", ""},
		{http.MethodPost, "/admin/schema-version", `{"enabled":true}`},
		{http.MethodPost, "/admin/outage", `{"enabled":true}`},
	}

	for _, ep := range endpoints {
		for _, tt := range tests {
			t.Run(ep.path+" "+tt.name, func(t *testing.T) {
				if rec := h.do(ep.method, ep.path, ep.body, tt.headers); rec.Code != tt.want {
					t.Fatalf("status = %d, want %d", rec.Code, tt.want)
				}
			})
		}
	}
	if h.store.SchemaEnabled() || h.store.Outage() {
		t.Fatal("an unauthenticated caller changed the mock's behaviour")
	}
	if len(h.slept) != 0 {
		t.Fatal("rejected requests must not incur the artificial delay")
	}
}

func TestVolcanicReportsAreServedWithAFluctuatingDelay(t *testing.T) {
	h := newHarness()

	reports := h.reports(t)
	if len(reports) != 20 {
		t.Fatalf("reports = %d", len(reports))
	}
	for _, field := range []string{"report_id", "volcano_id", "alert_level", "eruption_count_24h", "ash_column_height_m", "reported_at"} {
		if _, ok := reports[0][field]; !ok {
			t.Fatalf("contract field %q missing", field)
		}
	}
	if _, ok := reports[0]["confidence_level"]; ok {
		t.Fatal("the initial schema must not expose confidence_level")
	}

	for i := 0; i < 50; i++ {
		h.reports(t)
	}
	distinct := map[time.Duration]bool{}
	for _, d := range h.slept {
		if d < 500*time.Millisecond || d > 3*time.Second {
			t.Fatalf("delay %s outside the 500ms-3s contract", d)
		}
		distinct[d] = true
	}
	if len(h.slept) != 51 || len(distinct) < 10 {
		t.Fatalf("delays=%d distinct=%d; every request must wait a random time", len(h.slept), len(distinct))
	}
}

func TestSinceFiltersAndIsValidated(t *testing.T) {
	h := newHarness()
	tenth := h.store.ReportsSince(nil)[9].ReportedAt

	rec := h.do(http.MethodGet, "/volcanic-reports?since="+tenth.Format(time.RFC3339Nano), "", bearer())
	var reports []map[string]any
	json.Unmarshal(rec.Body.Bytes(), &reports)
	if rec.Code != http.StatusOK || len(reports) != 10 {
		t.Fatalf("since filter: %d reports=%d", rec.Code, len(reports))
	}

	before := len(h.slept)
	rec = h.do(http.MethodGet, "/volcanic-reports?since=yesterday", "", bearer())
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "RFC3339") || len(h.slept) != before {
		t.Fatalf("invalid since: %d %s", rec.Code, rec.Body.String())
	}

	rec = h.do(http.MethodGet, "/volcanic-reports?since=2999-01-01T00:00:00Z", "", bearer())
	if strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("an empty result must be [], got %q", rec.Body.String())
	}
}

func TestSchemaChangeAtRuntimeIsVisibleImmediatelyWithoutARestart(t *testing.T) {
	h := newHarness()

	for _, r := range h.reports(t) {
		if _, ok := r["confidence_level"]; ok {
			t.Fatal("old schema expected before the switch")
		}
	}

	rec := h.do(http.MethodPost, "/admin/schema-version", `{"enabled":true}`, bearer())
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"enabled":true}` {
		t.Fatalf("switch: %d %s", rec.Code, rec.Body.String())
	}

	reports := h.reports(t)
	if len(reports) != 21 {
		t.Fatalf("reports = %d, want 21", len(reports))
	}
	withField := 0
	for _, r := range reports {
		if v, ok := r["confidence_level"]; ok {
			withField++
			if f, _ := v.(float64); f < 0 || f > 1 {
				t.Fatalf("confidence_level %v must be within 0-1", v)
			}
		}
	}
	if withField != 1 || reports[20]["confidence_level"] == nil {
		t.Fatalf("exactly the newest report should carry the new field, got %d", withField)
	}

	rec = h.do(http.MethodPost, "/admin/schema-version", `{"enabled":false}`, bearer())
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"enabled":false}` {
		t.Fatalf("switch back: %d %s", rec.Code, rec.Body.String())
	}
}

func TestOutageIsInstantAndReversibleWithoutARestart(t *testing.T) {
	h := newHarness()

	if rec := h.do(http.MethodGet, "/health", "", nil); rec.Code != http.StatusOK {
		t.Fatalf("health before outage = %d", rec.Code)
	}

	rec := h.do(http.MethodPost, "/admin/outage", `{"enabled":true}`, bearer())
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"enabled":true}` {
		t.Fatalf("outage on: %d %s", rec.Code, rec.Body.String())
	}

	slept := len(h.slept)
	rec = h.do(http.MethodGet, "/volcanic-reports", "", bearer())
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "outage") {
		t.Fatalf("during the outage: %d %s", rec.Code, rec.Body.String())
	}
	if len(h.slept) != slept {
		t.Fatal("an outage must fail fast; it may not first wait out the random delay")
	}
	if rec := h.do(http.MethodGet, "/health", "", nil); rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"outage"`) {
		t.Fatalf("health during the outage: %d %s", rec.Code, rec.Body.String())
	}
	if rec := h.do(http.MethodPost, "/admin/schema-version", `{"enabled":true}`, bearer()); rec.Code != http.StatusOK {
		t.Fatalf("admin endpoints must stay reachable during an outage, got %d", rec.Code)
	}

	if rec := h.do(http.MethodPost, "/admin/outage", `{"enabled":false}`, bearer()); rec.Code != http.StatusOK {
		t.Fatalf("outage off: %d", rec.Code)
	}
	if rec := h.do(http.MethodGet, "/volcanic-reports", "", bearer()); rec.Code != http.StatusOK {
		t.Fatalf("after the outage: %d", rec.Code)
	}
	if rec := h.do(http.MethodGet, "/health", "", nil); rec.Code != http.StatusOK {
		t.Fatalf("health after the outage: %d", rec.Code)
	}
}

func TestAdminEndpointsValidateTheirBody(t *testing.T) {
	h := newHarness()

	bodies := map[string]string{
		"empty body":       "",
		"not json":         "enable please",
		"missing field":    `{}`,
		"wrong type":       `{"enabled":"yes"}`,
		"unknown field":    `{"enabled":true,"extra":1}`,
		"two json objects": `{"enabled":true}{"enabled":false}`,
		"trailing garbage": `{"enabled":true} nonsense`,
		"null value":       `{"enabled":null}`,
	}
	for _, path := range []string{"/admin/schema-version", "/admin/outage"} {
		for name, body := range bodies {
			t.Run(path+" "+name, func(t *testing.T) {
				rec := h.do(http.MethodPost, path, body, bearer())
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
				}
			})
		}
	}
	if h.store.SchemaEnabled() || h.store.Outage() {
		t.Fatal("a malformed request changed the mock's behaviour")
	}
}

func TestAdminEndpointsRejectOtherMethods(t *testing.T) {
	h := newHarness()
	for _, path := range []string{"/admin/schema-version", "/admin/outage"} {
		if rec := h.do(http.MethodGet, path, "", bearer()); rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("GET %s = %d, want 405", path, rec.Code)
		}
	}
	if rec := h.do(http.MethodPost, "/volcanic-reports", "", bearer()); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /volcanic-reports = %d, want 405", rec.Code)
	}
}

func TestRequestsAreLoggedWithTheCorrelationIDAndTheArtificialLatency(t *testing.T) {
	h := newHarness()
	headers := bearer()
	headers["X-Correlation-ID"] = "from-aggregator"

	rec := h.do(http.MethodGet, "/volcanic-reports", "", headers)
	if rec.Header().Get("X-Correlation-ID") != "from-aggregator" {
		t.Fatalf("correlation id header = %q", rec.Header().Get("X-Correlation-ID"))
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(h.logs.Bytes()), &record); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, h.logs.String())
	}
	if record["correlation_id"] != "from-aggregator" || record["service"] != "pvmbg-mock" || record["path"] != "/volcanic-reports" ||
		record["status"] != float64(200) || record["duration_ms"] == nil {
		t.Fatalf("log record = %v", record)
	}
	if strings.Contains(h.logs.String(), testToken) {
		t.Fatal("the token must never be logged")
	}

	rec = h.do(http.MethodGet, "/health", "", nil)
	id := rec.Header().Get("X-Correlation-ID")
	if _, err := hex.DecodeString(id); err != nil || len(id) != 32 {
		t.Fatalf("generated correlation id = %q", id)
	}
}

func TestNewServerWithoutALogWriterStillWorks(t *testing.T) {
	srv := NewServer(Config{Token: testToken, DelayMin: time.Millisecond, DelayMax: time.Millisecond}, NewStore(20, 1, fixedClock), nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}
