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

func observed(t *testing.T, handler http.HandlerFunc, header string) (*httptest.ResponseRecorder, string) {
	t.Helper()
	var logs bytes.Buffer
	h := Observe(slog.New(slog.NewJSONHandler(&logs, nil)))(handler)

	req := httptest.NewRequest(http.MethodGet, "/hazard-events", nil)
	if header != "" {
		req.Header.Set("X-Correlation-ID", header)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, logs.String()
}

func TestObserveEchoesAValidCorrelationID(t *testing.T) {
	var inContext string
	rec, logs := observed(t, func(w http.ResponseWriter, r *http.Request) {
		inContext = CorrelationID(r.Context())
		SetClient(r.Context(), "media-demo")
		w.WriteHeader(http.StatusAccepted)
	}, "demo-123.abc_X")

	if rec.Header().Get("X-Correlation-ID") != "demo-123.abc_X" || inContext != "demo-123.abc_X" {
		t.Fatalf("header=%q context=%q", rec.Header().Get("X-Correlation-ID"), inContext)
	}
	for _, want := range []string{`"correlation_id":"demo-123.abc_X"`, `"client":"media-demo"`, `"status":202`, `"duration_ms"`, `"method":"GET"`} {
		if !strings.Contains(logs, want) {
			t.Fatalf("log %q lacks %s", logs, want)
		}
	}
}

func TestObserveReplacesUnsafeOrMissingCorrelationIDs(t *testing.T) {
	for _, header := range []string{"", "has space", "line\nbreak", strings.Repeat("a", 65), "semi;colon", "<script>"} {
		rec, logs := observed(t, func(w http.ResponseWriter, _ *http.Request) {}, header)

		got := rec.Header().Get("X-Correlation-ID")
		if got == header && header != "" {
			t.Fatalf("unsafe id %q was accepted", header)
		}
		if _, err := hex.DecodeString(got); err != nil || len(got) != 16 {
			t.Fatalf("generated id %q should be 16 hex characters", got)
		}
		if header != "" && strings.Contains(logs, "script") {
			t.Fatal("an unsafe id must not reach the logs")
		}
	}
}

func TestObserveRecoversFromPanicsWithoutLeakingDetails(t *testing.T) {
	rec, logs := observed(t, func(http.ResponseWriter, *http.Request) {
		panic("secret internal detail")
	}, "panic-1")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] != "internal_error" {
		t.Fatalf("body = %q", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "secret internal detail") {
		t.Fatal("panic details must not reach the client")
	}
	for _, want := range []string{"panic in handler", `"correlation_id":"panic-1"`, `"status":500`} {
		if !strings.Contains(logs, want) {
			t.Fatalf("log %q lacks %s", logs, want)
		}
	}
}

func TestContextHelpersAreSafeWithoutObserve(t *testing.T) {
	if got := CorrelationID(context.Background()); got != "" {
		t.Fatalf("CorrelationID() = %q", got)
	}
	SetClient(context.Background(), "ignored") // must not panic
}

func TestWriteHelpers(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteJSON(rec, http.StatusOK, map[string]int{"n": 1})
	if rec.Header().Get("Content-Type") != "application/json" || !strings.Contains(rec.Body.String(), `"n":1`) {
		t.Fatalf("WriteJSON: %q %q", rec.Header().Get("Content-Type"), rec.Body.String())
	}

	rec = httptest.NewRecorder()
	WriteError(rec, http.StatusTooManyRequests, "too_many_requests", "slow down")
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != 429 || body["error"] != "too_many_requests" || body["message"] != "slow down" {
		t.Fatalf("WriteError: %d %q", rec.Code, rec.Body.String())
	}
}

func TestValidID(t *testing.T) {
	tests := map[string]bool{
		"": false, "abc": true, "A-b_c.9": true, "a b": false, "é": false, "a/b": false,
		strings.Repeat("x", 64): true, strings.Repeat("x", 65): false,
	}
	for in, want := range tests {
		if got := validID(in); got != want {
			t.Errorf("validID(%q) = %v, want %v", in, got, want)
		}
	}
}
