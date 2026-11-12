package api

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"client-facing-api/internal/authn"
	"client-facing-api/internal/upstream"
)

type fakePub struct {
	mu    sync.Mutex
	err   error
	delay time.Duration
	msgs  []fieldReportMessage
}

func (f *fakePub) PublishFieldReport(ctx context.Context, id string, body []byte, _ string) error {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.err != nil {
		return f.err
	}
	var m fieldReportMessage
	_ = json.Unmarshal(body, &m)
	f.mu.Lock()
	f.msgs = append(f.msgs, m)
	f.mu.Unlock()
	return nil
}

type m2Rig struct {
	routes http.Handler
	priv   ed25519.PrivateKey
	pub    *fakePub
	agg    *httptest.Server
	paths  []string
}

func newM2Rig(t *testing.T) *m2Rig {
	t.Helper()
	publicKey, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	r := &m2Rig{priv: privateKey, pub: &fakePub{}}
	var mu sync.Mutex
	r.agg = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		r.paths = append(r.paths, req.Method+" "+req.URL.RequestURI())
		mu.Unlock()
		if req.Header.Get("X-Internal-Key") != "k" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"items":[],"next_cursor":null}`))
	}))
	t.Cleanup(r.agg.Close)
	ops := upstream.New(r.agg.URL, "k", time.Second, 2, testLogger())
	identity := func(next http.Handler) http.Handler { return next }
	r.routes = NewHandler(nil).WithOps(ops).WithFieldReports(r.pub, 200*time.Millisecond).
		Routes(authn.NewVerifier(publicKey, "iss"), identity, identity)
	return r
}

func (r *m2Rig) do(t *testing.T, method, path, scope, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if scope != "" {
		req.Header.Set("Authorization", "Bearer "+signToken(t, r.priv, "iss", scope))
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	r.routes.ServeHTTP(rec, req)
	return rec
}

const (
	mediaScope = "read:summary"
	fieldScope = "read:summary read:raw write:field-reports"
	opsScope   = "read:summary read:raw ops:unprocessed"
)

func TestFieldReportAcceptedOnlyAfterBrokerConfirms(t *testing.T) {
	r := newM2Rig(t)
	rec := r.do(t, http.MethodPost, "/field-reports", fieldScope, `{"text":"jembatan putus","lat":-2.5}`, map[string]string{"Idempotency-Key": "abc-1"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if out["report_id"] != "test-client:abc-1" || len(r.pub.msgs) != 1 || r.pub.msgs[0].ClientID != "test-client" {
		t.Fatalf("%v %+v", out, r.pub.msgs)
	}

	r.pub.err = errors.New("broker down")
	rec = r.do(t, http.MethodPost, "/field-reports", fieldScope, `{"text":"x"}`, nil)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("tanpa konfirmasi broker tidak boleh 202: %d", rec.Code)
	}
}

func TestFieldReportFailsFastWhenBrokerIsSlow(t *testing.T) {
	r := newM2Rig(t)
	r.pub.delay = 5 * time.Second
	start := time.Now()
	rec := r.do(t, http.MethodPost, "/field-reports", fieldScope, `{"text":"x"}`, nil)
	if rec.Code != http.StatusServiceUnavailable || time.Since(start) > time.Second {
		t.Fatalf("status %d setelah %v", rec.Code, time.Since(start))
	}
}

func TestFieldReportValidationAndScope(t *testing.T) {
	r := newM2Rig(t)
	cases := []struct {
		scope, body string
		headers     map[string]string
		want        int
	}{
		{mediaScope, `{"text":"x"}`, nil, http.StatusForbidden},
		{opsScope, `{"text":"x"}`, nil, http.StatusForbidden},
		{fieldScope, `bukan json`, nil, http.StatusBadRequest},
		{fieldScope, `{}`, nil, http.StatusBadRequest},
		{fieldScope, "{\"t\":\"\xff\"}", nil, http.StatusBadRequest},
		{fieldScope, `{"t":"` + strings.Repeat("a", 17<<10) + `"}`, nil, http.StatusRequestEntityTooLarge},
		{fieldScope, `{"text":"x"}`, map[string]string{"Idempotency-Key": "spasi tidak boleh"}, http.StatusBadRequest},
	}
	for i, tc := range cases {
		if rec := r.do(t, http.MethodPost, "/field-reports", tc.scope, tc.body, tc.headers); rec.Code != tc.want {
			t.Fatalf("kasus %d: %d, ingin %d", i, rec.Code, tc.want)
		}
	}
	if len(r.pub.msgs) != 0 {
		t.Fatal("laporan yang ditolak tidak boleh diterbitkan")
	}
}

func TestOnlyInternalOpsCanInspectUnprocessed(t *testing.T) {
	r := newM2Rig(t)
	for _, scope := range []string{mediaScope, fieldScope} {
		if rec := r.do(t, http.MethodGet, "/ops/unprocessed", scope, "", nil); rec.Code != http.StatusForbidden {
			t.Fatalf("scope %q: %d", scope, rec.Code)
		}
	}
	if rec := r.do(t, http.MethodGet, "/ops/unprocessed?source=PVMBG&status=parked", opsScope, "", nil); rec.Code != http.StatusOK {
		t.Fatalf("ops: %d %s", rec.Code, rec.Body.String())
	}
	if rec := r.do(t, http.MethodPost, "/ops/unprocessed/reprocess?source=PVMBG", opsScope, "", nil); rec.Code != http.StatusOK {
		t.Fatalf("reprocess: %d", rec.Code)
	}
	if rec := r.do(t, http.MethodGet, "/ops/unprocessed/msg-abc", opsScope, "", nil); rec.Code != http.StatusOK {
		t.Fatalf("detail: %d", rec.Code)
	}
	want := []string{"GET /internal/unprocessed?source=PVMBG&status=parked", "POST /internal/unprocessed/reprocess?source=PVMBG", "GET /internal/unprocessed/msg-abc"}
	if len(r.paths) != 3 || r.paths[0] != want[0] || r.paths[1] != want[1] || r.paths[2] != want[2] {
		t.Fatalf("diteruskan ke %v", r.paths)
	}
}
