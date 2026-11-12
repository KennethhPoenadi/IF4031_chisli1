package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testKey = "sk-RAHASIA-uji-jangan-bocor-0123456789"

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClock() *clock { return &clock{t: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)} }

func okBody(text string) string {
	b, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"message": map[string]string{"content": text}}}})
	return string(b)
}

func newClient(t *testing.T, srv *httptest.Server, mod func(*Config), c *clock, logBuf *bytes.Buffer) *Client {
	t.Helper()
	cfg := Config{BaseURL: srv.URL + "/v1", Model: "deepseek/deepseek-v4-flash-free", APIKey: testKey,
		Timeout: 300 * time.Millisecond, MaxPerHour: 100, FailureThreshold: 3, OpenFor: time.Minute}
	if mod != nil {
		mod(&cfg)
	}
	var log *slog.Logger
	if logBuf != nil {
		log = slog.New(slog.NewJSONHandler(logBuf, nil))
	}
	cl, err := New(cfg, c.now, log)
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

func TestSuccessfulCallSendsTheExpectedRequest(t *testing.T) {
	var gotAuth, gotCT, gotPath string
	var gotBody chatRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotCT, gotPath = r.Header.Get("Authorization"), r.Header.Get("Content-Type"), r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, _ = w.Write([]byte(okBody("Ringkasan uji.")))
	}))
	defer srv.Close()

	cl := newClient(t, srv, nil, newClock(), nil)
	text, outcome, err := cl.Summarize(context.Background(), "sistem", "pengguna")
	if err != nil || outcome != OutcomeOK || text != "Ringkasan uji." {
		t.Fatalf("text=%q outcome=%s err=%v", text, outcome, err)
	}
	if gotPath != "/v1/chat/completions" || gotAuth != "Bearer "+testKey || gotCT != "application/json" {
		t.Fatalf("path=%q auth ok=%v ct=%q", gotPath, gotAuth == "Bearer "+testKey, gotCT)
	}
	if gotBody.Model != "deepseek/deepseek-v4-flash-free" || len(gotBody.Messages) != 2 ||
		gotBody.Messages[0].Role != "system" || gotBody.Messages[1].Content != "pengguna" || gotBody.MaxTokens != 300 {
		t.Fatalf("body = %+v", gotBody)
	}
}

func TestClientIsDisabledWithoutFullConfigAndNeverCallsOut(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer srv.Close()
	for name, mod := range map[string]func(*Config){
		"tanpa key":    func(c *Config) { c.APIKey = "" },
		"tanpa model":  func(c *Config) { c.Model = "" },
		"tanpa alamat": func(c *Config) { c.BaseURL = "" },
	} {
		cl := newClient(t, srv, mod, newClock(), nil)
		if cl.Enabled() {
			t.Fatalf("%s: klien seharusnya nonaktif", name)
		}
		if _, outcome, err := cl.Summarize(context.Background(), "s", "u"); outcome != OutcomeDisabled || err == nil {
			t.Fatalf("%s: outcome=%s err=%v", name, outcome, err)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("klien nonaktif melakukan %d panggilan", hits.Load())
	}
}

func TestAHangingLLMIsCutOffAtTheTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	cl := newClient(t, srv, func(c *Config) { c.Timeout = 150 * time.Millisecond }, newClock(), nil)
	start := time.Now()
	_, outcome, err := cl.Summarize(context.Background(), "s", "u")
	elapsed := time.Since(start)
	if outcome != OutcomeTimeout || err == nil {
		t.Fatalf("outcome=%s err=%v, want timeout", outcome, err)
	}
	if elapsed > time.Second {
		t.Fatalf("LLM yang macet menahan pemanggil %v, harus dipotong sekitar 150ms", elapsed)
	}
}

func TestBreakerOpensAfterRepeatedFailuresAndRecoversThroughAProbe(t *testing.T) {
	var hits atomic.Int32
	var failing atomic.Bool
	failing.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if failing.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(okBody("pulih")))
	}))
	defer srv.Close()

	clk := newClock()
	cl := newClient(t, srv, nil, clk, nil)
	for i := 0; i < 3; i++ {
		if _, outcome, _ := cl.Summarize(context.Background(), "s", "u"); outcome != OutcomeError {
			t.Fatalf("panggilan %d outcome=%s, want error", i, outcome)
		}
	}
	if !cl.BreakerOpen() {
		t.Fatal("breaker harus terbuka setelah 3 kegagalan beruntun")
	}

	before := hits.Load()
	start := time.Now()
	for i := 0; i < 50; i++ {
		if _, outcome, _ := cl.Summarize(context.Background(), "s", "u"); outcome != OutcomeBreakerOpen {
			t.Fatalf("outcome=%s, want breaker_open", outcome)
		}
	}
	if hits.Load() != before {
		t.Fatalf("breaker terbuka tetap menghubungi LLM %d kali", hits.Load()-before)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("menolak saat breaker terbuka harus seketika")
	}

	clk.advance(2 * time.Minute)
	failing.Store(false)
	if text, outcome, err := cl.Summarize(context.Background(), "s", "u"); outcome != OutcomeOK || text != "pulih" || err != nil {
		t.Fatalf("percobaan setelah masa terbuka: %q %s %v", text, outcome, err)
	}
	if cl.BreakerOpen() {
		t.Fatal("breaker harus tertutup lagi setelah percobaan berhasil")
	}
}

func TestAFailedProbeReopensTheBreaker(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer srv.Close()
	clk := newClock()
	cl := newClient(t, srv, nil, clk, nil)
	for i := 0; i < 3; i++ {
		cl.Summarize(context.Background(), "s", "u") //nolint:errcheck
	}
	clk.advance(2 * time.Minute)
	if _, outcome, _ := cl.Summarize(context.Background(), "s", "u"); outcome != OutcomeError {
		t.Fatalf("percobaan harus tetap dikirim, outcome=%s", outcome)
	}
	if _, outcome, _ := cl.Summarize(context.Background(), "s", "u"); outcome != OutcomeBreakerOpen {
		t.Fatalf("setelah percobaan gagal breaker harus terbuka lagi, outcome=%s", outcome)
	}
}

func TestHourlyBudgetProtectsTheFreeQuota(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(okBody("ok")))
	}))
	defer srv.Close()
	clk := newClock()
	cl := newClient(t, srv, func(c *Config) { c.MaxPerHour = 2 }, clk, nil)

	for i, want := range []Outcome{OutcomeOK, OutcomeOK, OutcomeBudget, OutcomeBudget} {
		if _, outcome, _ := cl.Summarize(context.Background(), "s", "u"); outcome != want {
			t.Fatalf("panggilan %d outcome=%s, want %s", i, outcome, want)
		}
	}
	if hits.Load() != 2 {
		t.Fatalf("hanya 2 panggilan boleh sampai ke LLM, dapat %d", hits.Load())
	}
	if cl.BreakerOpen() {
		t.Fatal("habisnya anggaran bukan kegagalan LLM dan tidak boleh membuka breaker")
	}
	clk.advance(61 * time.Minute)
	if _, outcome, _ := cl.Summarize(context.Background(), "s", "u"); outcome != OutcomeOK {
		t.Fatalf("anggaran harus pulih setelah satu jam, outcome=%s", outcome)
	}
}

func TestTheKeyNeverAppearsInLogsOrErrorsEvenIfTheProviderEchoesIt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "kunci ditolak: "+r.Header.Get("Authorization"), http.StatusUnauthorized)
	}))
	defer srv.Close()

	var logs bytes.Buffer
	cl := newClient(t, srv, nil, newClock(), &logs)
	_, outcome, err := cl.Summarize(context.Background(), "s", "u")
	if outcome != OutcomeError || err == nil {
		t.Fatalf("outcome=%s err=%v", outcome, err)
	}
	if strings.Contains(err.Error(), testKey) || strings.Contains(logs.String(), testKey) {
		t.Fatalf("key bocor!\nerror: %v\nlog: %s", err, logs.String())
	}
	if strings.Contains(logs.String(), "Bearer") {
		t.Fatalf("header Authorization muncul di log: %s", logs.String())
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("status harus tetap terlihat untuk diagnosis: %v", err)
	}
}

func TestRedirectsAreNotFollowedSoTheKeyNeverReachesAnotherHost(t *testing.T) {
	var leaked atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			leaked.Add(1)
		}
		_, _ = w.Write([]byte(okBody("dari tempat lain")))
	}))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/curi", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	cl := newClient(t, srv, nil, newClock(), nil)
	if _, outcome, _ := cl.Summarize(context.Background(), "s", "u"); outcome != OutcomeError {
		t.Fatalf("pengalihan harus dianggap kegagalan, outcome=%s", outcome)
	}
	if leaked.Load() != 0 {
		t.Fatal("key terkirim ke alamat hasil pengalihan")
	}
}

func TestMalformedAndOversizedResponsesAreRejected(t *testing.T) {
	bodies := map[string]string{
		"bukan json":    "ini bukan json",
		"tanpa pilihan": `{"choices":[]}`,
		"isi kosong":    okBody("   "),
		"terlalu besar": okBody(strings.Repeat("x", 2000)),
	}
	for name, body := range bodies {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
		cl := newClient(t, srv, func(c *Config) { c.MaxResponseBytes = 1024 }, newClock(), nil)
		if _, outcome, _ := cl.Summarize(context.Background(), "s", "u"); outcome != OutcomeBadResponse {
			t.Fatalf("%s: outcome=%s, want bad_response", name, outcome)
		}
		srv.Close()
	}
}

func TestConnectionRefusedIsAPlainFailureNotAPanic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()
	cl, err := New(Config{BaseURL: url, Model: "m", APIKey: testKey, Timeout: time.Second}, newClock().now, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, outcome, err := cl.Summarize(context.Background(), "s", "u")
	if outcome != OutcomeError || err == nil || strings.Contains(err.Error(), testKey) {
		t.Fatalf("outcome=%s err=%v", outcome, err)
	}
}

func TestNewRejectsUnsafeBaseURLs(t *testing.T) {
	bad := []string{
		"http://api.contoh.example/v1",
		"https://user:pass@api.contoh.example",
		"https://api.contoh.example/v1?key=abc",
		"ftp://api.contoh.example",
		"://rusak",
	}
	for _, u := range bad {
		if _, err := New(Config{BaseURL: u, Model: "m", APIKey: "k"}, nil, nil); err == nil {
			t.Fatalf("alamat %q seharusnya ditolak", u)
		}
	}
	for _, u := range []string{"https://api.orcarouter.ai/v1", "https://api.orcarouter.ai/v1/", "http://127.0.0.1:9999/v1", "http://localhost:1/v1"} {
		if _, err := New(Config{BaseURL: u, Model: "m", APIKey: "k"}, nil, nil); err != nil {
			t.Fatalf("alamat %q seharusnya diterima: %v", u, err)
		}
	}
}
