package api

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"client-facing-api/internal/authn"
	"client-facing-api/internal/httpx"
	"client-facing-api/internal/priority"
	"client-facing-api/internal/upstream"
)

const flowIssuer = "flow-test-auth"

// aggregatorEvent mimics what the real aggregator returns, including precise coordinates and an
// attributes blob with a marker that must never reach a summary-only client.
func aggregatorEvent() map[string]any {
	return map[string]any{
		"hazard_id": "hz-1", "source": "BMKG", "source_ref_id": "bmkg-000001", "hazard_type": "SEISMIC",
		"severity": "AWAS", "area_name": "Selat Sunda", "latitude": -6.123456, "longitude": 105.654321,
		"occurred_at": "2026-10-08T01:02:03Z", "ingested_at": "2026-10-08T01:02:05Z",
		"attributes": map[string]any{"magnitude": 7.1, "marker": "SECRET-ATTRIBUTE-MARKER"},
	}
}

type flow struct {
	routes     http.Handler
	privateKey ed25519.PrivateKey
	calls      atomic.Int32
	lastQuery  atomic.Value // url.Values
	lastHeader atomic.Value // http.Header
	delay      time.Duration
	blockFirst <-chan struct{}
}

func newFlow(t *testing.T) *flow {
	t.Helper()
	f := &flow{}
	aggregator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callNumber := f.calls.Add(1)
		f.lastQuery.Store(r.URL.Query())
		f.lastHeader.Store(r.Header.Clone())
		if f.blockFirst != nil && callNumber == 1 {
			<-f.blockFirst
		}
		if f.delay > 0 {
			time.Sleep(f.delay)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"data": []any{aggregatorEvent()},
			"meta": map[string]any{"sources": map[string]any{"pvmbg": map[string]any{"stale": true, "source_status": "DEGRADED"}}},
		})
	}))
	t.Cleanup(aggregator.Close)

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f.privateKey = priv
	client := upstream.New(aggregator.URL, "agg-key", 2*time.Second, 10, testLogger())
	admission, err := priority.New(priority.Config{
		MaxInFlight: 2, ReservedForPriority: 1, MaxPriorityQueued: 2,
		IsPriority: func(r *http.Request) bool {
			claims := authn.ClaimsFrom(r.Context())
			return claims != nil && claims.Subject == "field-demo"
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.routes = NewHandler(client).Routes(authn.NewVerifier(pub, flowIssuer), admission.Middleware, httpx.Observe(testLogger()))
	return f
}

func (f *flow) get(t *testing.T, scope, target string) *httptest.ResponseRecorder {
	return f.getAs(t, "test-client", scope, target)
}

func (f *flow) getAs(t *testing.T, subject, scope, target string) *httptest.ResponseRecorder {
	t.Helper()
	return f.serve(f.request(t, subject, scope, target))
}

func (f *flow) request(t *testing.T, subject, scope, target string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("Authorization", "Bearer "+signTokenForClient(t, f.privateKey, flowIssuer, scope, subject))
	req.Header.Set("X-Correlation-ID", "flow-corr-1")
	return req
}

func (f *flow) serve(req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	f.routes.ServeHTTP(rec, req)
	return rec
}

func TestPriorityAdmissionUsesVerifiedClientIdentity(t *testing.T) {
	f := newFlow(t)
	gate := make(chan struct{})
	gateClosed := false
	defer func() {
		if !gateClosed {
			close(gate)
		}
	}()
	f.blockFirst = gate
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	firstRequest := f.request(t, "media-demo", scopeSummary, "/hazard-events")
	go func() {
		firstDone <- f.serve(firstRequest)
	}()
	waitForCalls(t, f, 1)

	media := f.getAs(t, "media-demo", scopeSummary, "/hazard-events")
	if media.Code != http.StatusTooManyRequests || decode(t, media)["error"] != "too_many_requests" {
		t.Fatalf("media overload response = %d %s", media.Code, media.Body.String())
	}
	field := f.getAs(t, "field-demo", scopeSummary, "/hazard-events")
	if field.Code != http.StatusOK {
		t.Fatalf("priority response = %d %s", field.Code, field.Body.String())
	}
	close(gate)
	gateClosed = true
	select {
	case first := <-firstDone:
		if first.Code != http.StatusOK {
			t.Fatalf("first media response = %d", first.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the first media request")
	}
}

func waitForCalls(t *testing.T, f *flow, want int32) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if f.calls.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("aggregator calls did not reach %d", want)
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v\n%s", err, rec.Body.String())
	}
	return body
}

func TestMediaClientNeverReceivesRawFields(t *testing.T) {
	f := newFlow(t)

	rec := f.get(t, scopeSummary, "/hazard-events")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	for _, leaked := range []string{"latitude", "longitude", "attributes", "source_ref_id", "SECRET-ATTRIBUTE-MARKER", "105.654321", "-6.123456"} {
		if strings.Contains(rec.Body.String(), leaked) {
			t.Fatalf("raw data %q leaked to a summary-only client: %s", leaked, rec.Body.String())
		}
	}

	body := decode(t, rec)
	events := body["data"].([]any)
	event := events[0].(map[string]any)
	for _, want := range summaryFields {
		if _, ok := event[want]; !ok {
			t.Fatalf("summary field %q missing: %v", want, event)
		}
	}
	if body["count"] != float64(1) {
		t.Fatalf("count = %v", body["count"])
	}
}

func TestMediaClientExplicitRawRequestIsRejectedServerSide(t *testing.T) {
	f := newFlow(t)

	for _, fields := range []string{"latitude", "attributes", "hazard_id,longitude", "source_ref_id"} {
		before := f.calls.Load()
		rec := f.get(t, scopeSummary, "/hazard-events?fields="+url.QueryEscape(fields))

		if rec.Code != http.StatusForbidden || decode(t, rec)["error"] != "forbidden_field" {
			t.Fatalf("fields=%q: %d %s, want 403 forbidden_field", fields, rec.Code, rec.Body.String())
		}
		if f.calls.Load() != before {
			t.Fatalf("fields=%q: a forbidden request must be refused before any data is fetched", fields)
		}
	}
}

func TestPrivilegedClientReceivesRawFields(t *testing.T) {
	f := newFlow(t)

	rec := f.get(t, scopeSummary+" "+scopeRaw, "/hazard-events")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	event := decode(t, rec)["data"].([]any)[0].(map[string]any)
	for _, want := range append(append([]string{}, summaryFields...), rawFields...) {
		if _, ok := event[want]; !ok {
			t.Fatalf("field %q missing for a client with raw access: %v", want, event)
		}
	}

	rec = f.get(t, scopeSummary+" "+scopeRaw, "/hazard-events?fields=hazard_id,latitude")
	event = decode(t, rec)["data"].([]any)[0].(map[string]any)
	if len(event) != 2 || event["latitude"] == nil {
		t.Fatalf("an explicit field list must be honoured exactly, got %v", event)
	}
}

func TestInvalidFieldAndQueryParametersAreRejected(t *testing.T) {
	f := newFlow(t)

	tests := []struct {
		target    string
		wantError string
	}{
		{target: "/hazard-events?fields=does_not_exist", wantError: "unknown_field"},
		{target: "/hazard-events?fields=,,", wantError: "invalid_fields"},
		{target: "/hazard-events?limit=0", wantError: "invalid_query"},
		{target: "/hazard-events?limit=201", wantError: "invalid_query"},
		{target: "/hazard-events?limit=abc", wantError: "invalid_query"},
		{target: "/hazard-events?hazard_type=FLOOD", wantError: "invalid_query"},
		{target: "/hazard-events?severity=KRITIS", wantError: "invalid_query"},
		{target: "/hazard-events?since=yesterday", wantError: "invalid_query"},
	}

	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			before := f.calls.Load()
			rec := f.get(t, scopeSummary+" "+scopeRaw, tt.target)
			if rec.Code != http.StatusBadRequest || decode(t, rec)["error"] != tt.wantError {
				t.Fatalf("%d %s, want 400 %s", rec.Code, rec.Body.String(), tt.wantError)
			}
			if f.calls.Load() != before {
				t.Fatal("an invalid request must not reach the aggregator")
			}
		})
	}
}

func TestValidQueryIsForwardedWithCredentialsAndCorrelationID(t *testing.T) {
	f := newFlow(t)

	rec := f.get(t, scopeSummary, "/hazard-events?limit=7&hazard_type=VOLCANIC&severity=SIAGA&since=2026-10-08T01:02:03Z")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}

	query := f.lastQuery.Load().(url.Values)
	if query.Get("limit") != "7" || query.Get("hazard_type") != "VOLCANIC" || query.Get("severity") != "SIAGA" || query.Get("since") != "2026-10-08T01:02:03Z" {
		t.Fatalf("aggregator received %v", query)
	}
	if query.Get("fields") != "" {
		t.Fatal("the field list is enforced here and must not be delegated to the aggregator")
	}
	header := f.lastHeader.Load().(http.Header)
	if header.Get("X-Internal-Key") != "agg-key" || header.Get("X-Correlation-ID") != "flow-corr-1" {
		t.Fatalf("aggregator headers = %v", header)
	}
	if strings.Contains(header.Get("Authorization"), "Bearer") {
		t.Fatal("the client's token must not be forwarded to the aggregator")
	}
	if rec.Header().Get("X-Correlation-ID") != "flow-corr-1" {
		t.Fatal("the correlation id must be returned to the client")
	}
}

func TestFreshnessMetadataIsPassedThroughForDegradedSources(t *testing.T) {
	f := newFlow(t)

	body := decode(t, f.get(t, scopeSummary, "/hazard-events"))
	meta := body["meta"].(map[string]any)["sources"].(map[string]any)["pvmbg"].(map[string]any)
	if meta["stale"] != true || meta["source_status"] != "DEGRADED" {
		t.Fatalf("meta = %v; clients must be told that volcanic data is stale", body["meta"])
	}
}

func TestSlowAggregatorYieldsServiceUnavailableNotAHang(t *testing.T) {
	f := newFlow(t)
	f.delay = 300 * time.Millisecond

	req := httptest.NewRequest(http.MethodGet, "/hazard-events", nil)
	req.Header.Set("Authorization", "Bearer "+signToken(t, f.privateKey, flowIssuer, scopeSummary))
	ctx, cancel := context.WithTimeout(req.Context(), 40*time.Millisecond)
	defer cancel()
	rec := httptest.NewRecorder()

	started := time.Now()
	f.routes.ServeHTTP(rec, req.WithContext(ctx))

	if rec.Code != http.StatusServiceUnavailable || decode(t, rec)["error"] != "upstream_unavailable" {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "did not answer in time") {
		t.Fatalf("a deadline must be reported as such: %s", rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "2" || time.Since(started) > 250*time.Millisecond {
		t.Fatalf("retry-after=%q elapsed=%s", rec.Header().Get("Retry-After"), time.Since(started))
	}
}

func TestBuildUpstreamQueryAcceptsTheDocumentedParameters(t *testing.T) {
	got, err := buildUpstreamQuery(url.Values{"limit": {"200"}, "hazard_type": {"SEISMIC"}, "severity": {"NORMAL"}, "since": {"2026-10-08T01:02:03+07:00"}, "fields": {"hazard_id"}})
	if err != nil {
		t.Fatalf("buildUpstreamQuery() error = %v", err)
	}
	if got.Get("limit") != "200" || got.Get("hazard_type") != "SEISMIC" || got.Get("severity") != "NORMAL" || got.Get("since") == "" {
		t.Fatalf("query = %v", got)
	}
	if got.Has("fields") {
		t.Fatal("fields is a client-facing concern and is not forwarded")
	}

	if empty, err := buildUpstreamQuery(url.Values{}); err != nil || len(empty) != 0 {
		t.Fatalf("empty query: %v %v", empty, err)
	}
}

func TestBuildUpstreamQueryForwardsCursorAndAcceptsNewHazardTypes(t *testing.T) {
	got, err := buildUpstreamQuery(url.Values{"cursor": {"eyJ0IjoiMjAyNiJ9"}, "hazard_type": {"WILDFIRE"}})
	if err != nil {
		t.Fatalf("buildUpstreamQuery() error = %v", err)
	}
	if got.Get("cursor") != "eyJ0IjoiMjAyNiJ9" || got.Get("hazard_type") != "WILDFIRE" {
		t.Fatalf("got %v, cursor dan hazard_type harus diteruskan", got)
	}
	if _, err := buildUpstreamQuery(url.Values{"hazard_type": {"COASTAL"}}); err != nil {
		t.Fatalf("COASTAL harus diterima: %v", err)
	}
	if _, err := buildUpstreamQuery(url.Values{"cursor": {strings.Repeat("a", 513)}}); err == nil {
		t.Fatal("cursor terlalu panjang harus ditolak")
	}
}
