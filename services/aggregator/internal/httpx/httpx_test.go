package httpx

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequireInternalKey(t *testing.T) {
	const key = "expected-internal-key"
	called := false
	handler := RequireInternalKey(key)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	tests := []struct {
		name       string
		header     string
		wantStatus int
		wantCalled bool
	}{
		{name: "missing", header: "", wantStatus: http.StatusUnauthorized},
		{name: "wrong", header: "another-key", wantStatus: http.StatusForbidden},
		{name: "same prefix only", header: "expected-internal-ke", wantStatus: http.StatusForbidden},
		{name: "correct", header: key, wantStatus: http.StatusNoContent, wantCalled: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.header != "" {
				req.Header.Set("X-Internal-Key", tt.header)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus || called != tt.wantCalled {
				t.Fatalf("status=%d called=%v, want %d/%v", rec.Code, called, tt.wantStatus, tt.wantCalled)
			}
			if strings.Contains(rec.Body.String(), key) {
				t.Fatal("the expected key must never be echoed back")
			}
		})
	}
}

func TestObservePropagatesAnIncomingCorrelationID(t *testing.T) {
	var logs bytes.Buffer
	var seenInContext string
	handler := Observe(slog.New(slog.NewJSONHandler(&logs, nil)))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenInContext = CorrelationID(r.Context())
		w.WriteHeader(http.StatusTeapot)
	}))

	req := httptest.NewRequest(http.MethodGet, "/hazard-events", nil)
	req.Header.Set("X-Correlation-ID", "from-client-api")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if seenInContext != "from-client-api" || rec.Header().Get("X-Correlation-ID") != "from-client-api" {
		t.Fatalf("context=%q header=%q", seenInContext, rec.Header().Get("X-Correlation-ID"))
	}
	for _, want := range []string{`"correlation_id":"from-client-api"`, `"status":418`, `"duration_ms"`, `"path":"/hazard-events"`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log %q lacks %s", logs.String(), want)
		}
	}
}

func TestObserveCreatesACorrelationIDWhenMissing(t *testing.T) {
	var logs bytes.Buffer
	handler := Observe(slog.New(slog.NewJSONHandler(&logs, nil)))(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	id := rec.Header().Get("X-Correlation-ID")
	if _, err := hex.DecodeString(id); err != nil || len(id) != 32 {
		t.Fatalf("generated id %q is not 32 hex characters", id)
	}
	if !strings.Contains(logs.String(), `"correlation_id":"`+id+`"`) || !strings.Contains(logs.String(), `"status":200`) {
		t.Fatalf("log %q", logs.String())
	}
}

func TestCorrelationIDIsEmptyOutsideARequest(t *testing.T) {
	if got := CorrelationID(context.Background()); got != "" {
		t.Fatalf("CorrelationID() = %q, want empty", got)
	}
}

func TestWriteJSONAndWriteError(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteJSON(rec, http.StatusCreated, map[string]int{"n": 1})
	if rec.Code != http.StatusCreated || rec.Header().Get("Content-Type") != "application/json" || !strings.Contains(rec.Body.String(), `"n":1`) {
		t.Fatalf("WriteJSON: %d %q %q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}

	rec = httptest.NewRecorder()
	WriteError(rec, http.StatusForbidden, "nope")
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != http.StatusForbidden || body["error"] != "nope" {
		t.Fatalf("WriteError: %d %q", rec.Code, rec.Body.String())
	}
}
