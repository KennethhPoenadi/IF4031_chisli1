package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"client-facing-api/internal/authn"
	"client-facing-api/internal/upstream"
)

func TestRoutesAuthenticateAndAuthorizeBeforeLimiter(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	const issuer = "test-auth"
	verifier := authn.NewVerifier(publicKey, issuer)

	var limiterCalls atomic.Int32
	limit := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			limiterCalls.Add(1)
			http.Error(w, "limited", http.StatusTooManyRequests)
		})
	}
	identity := func(next http.Handler) http.Handler { return next }
	routes := NewHandler(nil).Routes(verifier, limit, identity)

	tests := []struct {
		name       string
		token      string
		wantStatus int
		wantCalls  int32
	}{
		{name: "missing token", wantStatus: http.StatusUnauthorized, wantCalls: 0},
		{name: "missing scope", token: signToken(t, privateKey, issuer, "read:raw"), wantStatus: http.StatusForbidden, wantCalls: 0},
		{name: "authorized", token: signToken(t, privateKey, issuer, scopeSummary), wantStatus: http.StatusTooManyRequests, wantCalls: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/hazard-events", nil)
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			recorder := httptest.NewRecorder()
			routes.ServeHTTP(recorder, req)
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
			if got := limiterCalls.Load(); got != tt.wantCalls {
				t.Fatalf("limiter calls = %d, want %d", got, tt.wantCalls)
			}
		})
	}
}

func TestGetHazardEventsReturnsServiceUnavailableForUpstreamFailure(t *testing.T) {
	aggregator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusInternalServerError)
	}))
	defer aggregator.Close()

	client := upstream.New(aggregator.URL, "test-key", time.Second, 1, testLogger())
	handler := NewHandler(client)
	req := httptest.NewRequest(http.MethodGet, "/hazard-events", nil)
	req = req.WithContext(authn.WithClaims(req.Context(), &authn.Claims{Scope: scopeSummary}))
	recorder := httptest.NewRecorder()

	handler.GetHazardEvents(recorder, req)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if got := recorder.Header().Get("Retry-After"); got != "2" {
		t.Errorf("Retry-After = %q, want %q", got, "2")
	}
	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := body["error"]; got != "upstream_unavailable" {
		t.Errorf("error = %q, want %q", got, "upstream_unavailable")
	}
}

func signToken(t *testing.T, privateKey ed25519.PrivateKey, issuer, scope string) string {
	t.Helper()
	now := time.Now()
	claims := authn.Claims{
		Scope: scope,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    issuer,
			Subject:   "test-client",
			ID:        "test-token-" + scope,
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
			IssuedAt:  jwt.NewNumericDate(now),
		},
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(privateKey)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
