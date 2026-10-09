package authn

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func logRecords(t *testing.T, buf *syncBuffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		out = append(out, rec)
	}
	return out
}

func TestAuthServiceCallsAreLoggedWithLatencyAndTheCorrelationIDTheServerReceived(t *testing.T) {
	pub, _ := newKeyPair(t)
	var mu sync.Mutex
	received := map[string]string{} // path -> correlation id seen by the auth-service

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		received[r.URL.Path] = r.Header.Get("X-Correlation-ID")
		mu.Unlock()
		switch r.URL.Path {
		case "/public-key":
			json.NewEncoder(w).Encode(map[string]string{"alg": "EdDSA", "public_key": base64.StdEncoding.EncodeToString(pub)})
		case "/revocations":
			w.Write([]byte(`{"revoked":[]}`))
		}
	}))
	defer srv.Close()

	logs := &syncBuffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))

	if _, err := fetchOnce(context.Background(), srv.Client(), srv.URL, log); err != nil {
		t.Fatalf("fetchOnce() error = %v", err)
	}
	if _, err := fetchRevocations(context.Background(), srv.Client(), srv.URL, testInternalKey, log); err != nil {
		t.Fatalf("fetchRevocations() error = %v", err)
	}

	byOperation := map[string]map[string]any{}
	for _, rec := range logRecords(t, logs) {
		if rec["msg"] == "auth-service call" {
			byOperation[rec["operation"].(string)] = rec
		}
	}
	for operation, path := range map[string]string{"get_public_key": "/public-key", "get_revocations": "/revocations"} {
		rec, ok := byOperation[operation]
		if !ok {
			t.Fatalf("no latency log for %s, got %v", operation, byOperation)
		}
		if rec["target"] != "auth-service" || rec["success"] != true || rec["status"] != float64(200) {
			t.Fatalf("%s: %v", operation, rec)
		}
		if d, ok := rec["duration_ms"].(float64); !ok || d < 0 {
			t.Fatalf("%s: duration_ms missing or invalid: %v", operation, rec["duration_ms"])
		}
		id, _ := rec["correlation_id"].(string)
		mu.Lock()
		seen := received[path]
		mu.Unlock()
		if len(id) != 16 || id != seen {
			t.Fatalf("%s: logged correlation id %q must equal the one the auth-service received %q", operation, id, seen)
		}
	}
	if strings.Contains(logs.String(), testInternalKey) {
		t.Fatal("the internal key must never be logged")
	}
}

func TestFailedAuthServiceCallsAreLoggedWithLatencyToo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	target := srv.URL
	srv.Close()

	logs := &syncBuffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))

	if _, err := fetchRevocations(context.Background(), http.DefaultClient, target, testInternalKey, log); err == nil {
		t.Fatal("expected a connection error")
	}

	records := logRecords(t, logs)
	if len(records) != 1 {
		t.Fatalf("expected exactly one log line, got %d: %v", len(records), records)
	}
	rec := records[0]
	if rec["msg"] != "auth-service call failed" || rec["level"] != "WARN" || rec["success"] != false ||
		rec["operation"] != "get_revocations" || rec["err"] == nil || rec["duration_ms"] == nil || rec["correlation_id"] == "" {
		t.Fatalf("unexpected failure log: %v", rec)
	}
}

func TestAnUnexpectedStatusIsLoggedAsAnUnsuccessfulCall(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	defer srv.Close()

	logs := &syncBuffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))
	if _, err := fetchRevocations(context.Background(), srv.Client(), srv.URL, "wrong-key", log); err == nil {
		t.Fatal("expected an error for a 401")
	}

	rec := logRecords(t, logs)[0]
	if rec["status"] != float64(401) || rec["success"] != false || rec["duration_ms"] == nil {
		t.Fatalf("a rejected call must be logged as unsuccessful with its latency: %v", rec)
	}
}
