package audit

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/shardstore"
)

const token = "audit-token-for-test"

type fakeStore struct {
	page     shardstore.Page
	pageErr  error
	checksum shardstore.ChecksumResult
	location shardstore.LocationResult
	locErr   error
	gotQuery shardstore.ListQuery
	gotSrc   string
}

func (f *fakeStore) ListEvents(_ context.Context, q shardstore.ListQuery) (shardstore.Page, error) {
	f.gotQuery = q
	return f.page, f.pageErr
}

func (f *fakeStore) Checksum(_ context.Context, source string) (shardstore.ChecksumResult, error) {
	f.gotSrc = source
	return f.checksum, nil
}

func (f *fakeStore) Location(context.Context, string) (shardstore.LocationResult, error) {
	return f.location, f.locErr
}

func routes(s Store, u UnprocessedLister) http.Handler {
	mux := http.NewServeMux()
	New(s, u, token).Register(mux)
	return mux
}

func do(h http.Handler, target, auth string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("bukan JSON: %v\n%s", err, rec.Body.String())
	}
	return body
}

func TestEveryEndpointRequiresTheBearerToken(t *testing.T) {
	h := routes(&fakeStore{}, nil)
	for _, path := range []string{"/internal/audit/events", "/internal/audit/checksum?source=BMKG",
		"/internal/audit/location?hazard_id=x", "/internal/audit/unprocessed"} {
		for _, auth := range []string{"", "Bearer salah", "audit-token-for-test", "Basic " + token} {
			rec := do(h, path, auth)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("%s dengan %q: status %d, want 401", path, auth, rec.Code)
			}
			body := decode(t, rec)
			errObj, _ := body["error"].(map[string]any)
			if errObj["code"] != "unauthorized" || errObj["message"] != "token tidak valid" {
				t.Fatalf("bentuk error = %v", body)
			}
		}
	}
}

func TestAuditIsDisabledWhenNoTokenIsConfigured(t *testing.T) {
	mux := http.NewServeMux()
	New(&fakeStore{}, nil, "").Register(mux)
	if rec := do(mux, "/internal/audit/events", "Bearer "); rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 saat token tidak dikonfigurasi", rec.Code)
	}
}

func TestEventsReturnsTheContractShapeAndOldestFirst(t *testing.T) {
	ingested := time.Date(2026, 11, 12, 2, 51, 8, 0, time.FixedZone("WIB", 7*3600))
	store := &fakeStore{page: shardstore.Page{
		Events: []hazard.Event{{HazardID: "hz-1", Source: hazard.SourceSatelit, SourceRefID: "fire-1",
			HazardType: hazard.TypeWildfire, Severity: hazard.SeveritySiaga, AreaName: "Kalimantan Tengah",
			Latitude: -2.51234, Longitude: 113.8045, OccurredAt: ingested.Add(-8 * time.Minute), IngestedAt: ingested}},
		NextCursor: "abc",
	}}
	rec := do(routes(store, nil), "/internal/audit/events?source=SATELIT&since=2026-11-01T00:00:00Z&limit=5", "Bearer "+token)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	body := decode(t, rec)
	for _, key := range []string{"items", "next_cursor", "partial", "unavailable_units"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("field %q hilang dari respons: %v", key, body)
		}
	}
	items := body["items"].([]any)
	item := items[0].(map[string]any)
	for _, key := range []string{"hazard_id", "source", "source_ref_id", "hazard_type", "severity", "area_name",
		"latitude", "longitude", "occurred_at", "ingested_at", "attributes"} {
		if _, ok := item[key]; !ok {
			t.Fatalf("field event %q hilang: %v", key, item)
		}
	}
	if item["ingested_at"] != "2026-11-11T19:51:08Z" {
		t.Fatalf("ingested_at = %v, harus UTC", item["ingested_at"])
	}
	if body["next_cursor"] != "abc" || body["partial"] != false {
		t.Fatalf("meta = %v", body)
	}
	if store.gotQuery.Order != shardstore.OldestFirst || store.gotQuery.Limit != 5 ||
		store.gotQuery.Filter.Source != "SATELIT" || store.gotQuery.Filter.IngestedSince == nil {
		t.Fatalf("query = %+v", store.gotQuery)
	}
}

func TestEventsLastPageHasNullCursorAndReportsPartialUnits(t *testing.T) {
	store := &fakeStore{page: shardstore.Page{Partial: true, Unavailable: []string{"store-1"}}}
	body := decode(t, do(routes(store, nil), "/internal/audit/events", "Bearer "+token))
	if body["next_cursor"] != nil {
		t.Fatalf("next_cursor = %v, want null", body["next_cursor"])
	}
	if body["partial"] != true || len(body["unavailable_units"].([]any)) != 1 {
		t.Fatalf("partial/unavailable = %v", body)
	}
	if items, ok := body["items"].([]any); !ok || len(items) != 0 {
		t.Fatalf("items harus array kosong, dapat %v", body["items"])
	}
}

func TestEventsValidatesParameters(t *testing.T) {
	h := routes(&fakeStore{}, nil)
	for _, q := range []string{"limit=0", "limit=1001", "limit=abc", "source=NASA", "since=kemarin"} {
		rec := do(h, "/internal/audit/events?"+q, "Bearer "+token)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status %d, want 400", q, rec.Code)
		}
		if decode(t, rec)["error"].(map[string]any)["code"] != "bad_request" {
			t.Fatalf("%s: kode error salah", q)
		}
	}
}

func TestEventsMapsStoreFailuresToTheErrorFormat(t *testing.T) {
	tests := []struct {
		err  error
		want int
		code string
	}{
		{shardstore.ErrBadCursor, http.StatusBadRequest, "bad_request"},
		{shardstore.ErrUnavailable, http.StatusServiceUnavailable, "unavailable"},
		{errors.New("boom"), http.StatusServiceUnavailable, "unavailable"},
	}
	for _, tt := range tests {
		rec := do(routes(&fakeStore{pageErr: tt.err}, nil), "/internal/audit/events", "Bearer "+token)
		if rec.Code != tt.want || decode(t, rec)["error"].(map[string]any)["code"] != tt.code {
			t.Fatalf("err=%v: status %d body %s", tt.err, rec.Code, rec.Body.String())
		}
		if rec.Code == http.StatusInternalServerError {
			t.Fatal("tidak boleh 500")
		}
	}
}

func TestChecksumContractShape(t *testing.T) {
	store := &fakeStore{checksum: shardstore.ChecksumResult{Count: 12843, Checksum: "7e18f7", AsOf: time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)}}
	rec := do(routes(store, nil), "/internal/audit/checksum?source=SATELIT", "Bearer "+token)
	body := decode(t, rec)
	if rec.Code != http.StatusOK || body["source"] != "SATELIT" || body["count"] != float64(12843) ||
		body["checksum"] != "7e18f7" || body["algorithm"] != "sha256" || body["as_of"] != "2026-11-12T03:00:00Z" ||
		body["partial"] != false {
		t.Fatalf("respons = %d %v", rec.Code, body)
	}
	if store.gotSrc != "SATELIT" {
		t.Fatalf("source diteruskan = %q", store.gotSrc)
	}
	if rec := do(routes(store, nil), "/internal/audit/checksum?source=NASA", "Bearer "+token); rec.Code != http.StatusBadRequest {
		t.Fatalf("source tidak dikenal: status %d, want 400", rec.Code)
	}
}

func TestLocationFoundNotFoundAndPartial(t *testing.T) {
	found := &fakeStore{location: shardstore.LocationResult{Found: true, Locations: []shardstore.Placement{
		{Node: "node-b", Unit: "store-2", Role: "primary"}, {Node: "node-a", Unit: "store-0", Role: "replica"}}}}
	body := decode(t, do(routes(found, nil), "/internal/audit/location?hazard_id=hz-1", "Bearer "+token))
	locs := body["locations"].([]any)
	if len(locs) != 2 || locs[0].(map[string]any)["role"] != "primary" || locs[0].(map[string]any)["node"] != "node-b" ||
		body["hazard_id"] != "hz-1" || body["partial"] != false {
		t.Fatalf("respons = %v", body)
	}

	rec := do(routes(&fakeStore{}, nil), "/internal/audit/location?hazard_id=hz-x", "Bearer "+token)
	if rec.Code != http.StatusNotFound || decode(t, rec)["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("tidak ada: status %d body %s", rec.Code, rec.Body.String())
	}

	// tidak ditemukan tetapi sebagian unit tidak terjangkau: tidak boleh mengaku 404
	partial := &fakeStore{location: shardstore.LocationResult{Partial: true, Unavailable: []string{"store-1"}}}
	rec = do(routes(partial, nil), "/internal/audit/location?hazard_id=hz-x", "Bearer "+token)
	body = decode(t, rec)
	if rec.Code != http.StatusOK || body["partial"] != true {
		t.Fatalf("partial: status %d body %v", rec.Code, body)
	}

	if rec := do(routes(found, nil), "/internal/audit/location", "Bearer "+token); rec.Code != http.StatusBadRequest {
		t.Fatalf("tanpa hazard_id: status %d, want 400", rec.Code)
	}
}

type fakeUnprocessed struct{ items []Message }

func (f fakeUnprocessed) ListUnprocessed(context.Context, string, int, string) ([]Message, string, error) {
	return f.items, "", nil
}

func TestUnprocessedReturnsMessagesAndDefaultsToEmpty(t *testing.T) {
	msgs := fakeUnprocessed{items: []Message{{MessageID: "msg-1", Source: "PVMBG", Reason: "schema_incompatible",
		Retryable: false, Attempts: 3, Status: "parked", ReceivedAt: time.Date(2026, 11, 12, 2, 40, 11, 0, time.UTC)}}}
	body := decode(t, do(routes(&fakeStore{}, msgs), "/internal/audit/unprocessed", "Bearer "+token))
	item := body["items"].([]any)[0].(map[string]any)
	if item["message_id"] != "msg-1" || item["status"] != "parked" || item["retryable"] != false ||
		item["received_at"] != "2026-11-12T02:40:11Z" || body["next_cursor"] != nil {
		t.Fatalf("respons = %v", body)
	}

	empty := decode(t, do(routes(&fakeStore{}, nil), "/internal/audit/unprocessed", "Bearer "+token))
	if items, ok := empty["items"].([]any); !ok || len(items) != 0 {
		t.Fatalf("default harus array kosong, dapat %v", empty["items"])
	}
}

func TestFailedAuthIsLoggedWithoutTheTokenAndJSONHasSecurityHeaders(t *testing.T) {
	var buf strings.Builder
	mux := http.NewServeMux()
	New(&fakeStore{}, nil, token).WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))).Register(mux)

	do(mux, "/internal/audit/events", "Bearer tebakan-token-rahasia")
	logged := buf.String()
	if !strings.Contains(logged, "autentikasi audit api gagal") {
		t.Fatalf("percobaan gagal tidak tercatat: %q", logged)
	}
	if strings.Contains(logged, "tebakan-token-rahasia") || strings.Contains(logged, token) {
		t.Fatalf("log tidak boleh memuat token: %s", logged)
	}

	rec := do(mux, "/internal/audit/checksum", "Bearer "+token)
	if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("header respons audit kurang: %v", rec.Header())
	}
}
