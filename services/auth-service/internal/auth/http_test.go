package auth

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

const testInternalKey = "internal-key-for-http-tests"

type httpEnv struct {
	*testEnv
	handler http.Handler
	logs    *bytes.Buffer
}

func newHTTPEnv(t *testing.T) *httpEnv {
	t.Helper()
	env := newTestService(t)
	logs := &bytes.Buffer{}
	h := NewHandler(env.svc, testInternalKey, slog.New(slog.NewJSONHandler(logs, nil)))
	return &httpEnv{testEnv: env, handler: h.Routes(), logs: logs}
}

func (e *httpEnv) postToken(t *testing.T, values url.Values, remoteAddr string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(values.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if remoteAddr != "" {
		req.RemoteAddr = remoteAddr
	}
	rec := httptest.NewRecorder()
	e.handler.ServeHTTP(rec, req)
	return rec
}

func (e *httpEnv) loginForm(clientID, secret string) url.Values {
	return url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {secret}}
}

func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v\n%s", err, rec.Body.String())
	}
	return body
}

func TestHealthEndpoint(t *testing.T) {
	env := newHTTPEnv(t)
	rec := httptest.NewRecorder()
	env.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK || decodeJSON(t, rec)["status"] != "ok" {
		t.Fatalf("health: %d %s", rec.Code, rec.Body.String())
	}
}

func TestTokenEndpointIssuesTokensWithoutCaching(t *testing.T) {
	env := newHTTPEnv(t)

	rec := env.postToken(t, env.loginForm("field-test", env.secrets["field-test"]), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	body := decodeJSON(t, rec)
	for _, key := range []string{"access_token", "refresh_token", "token_type", "expires_in", "scope"} {
		if body[key] == nil || body[key] == "" {
			t.Fatalf("response lacks %s: %v", key, body)
		}
	}
	if body["scope"] != "read:summary read:raw" || body["expires_in"] != float64(60) {
		t.Fatalf("scope=%v expires_in=%v", body["scope"], body["expires_in"])
	}
	if _, leaked := body["ClientID"]; leaked || strings.Contains(rec.Body.String(), "field-test") {
		t.Fatal("the internal client id field must never be serialised")
	}
	if rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("Pragma") != "no-cache" {
		t.Fatalf("token responses must not be cacheable: %v", rec.Header())
	}
	if !strings.Contains(env.logs.String(), `"msg":"token issued"`) {
		t.Fatalf("issuing a token must be logged: %s", env.logs.String())
	}
	if strings.Contains(env.logs.String(), body["access_token"].(string)) || strings.Contains(env.logs.String(), env.secrets["field-test"]) {
		t.Fatal("tokens and secrets must never appear in logs")
	}
}

func TestTokenEndpointRejectsBadRequests(t *testing.T) {
	env := newHTTPEnv(t)

	tests := []struct {
		name       string
		values     url.Values
		wantStatus int
		wantError  string
	}{
		{name: "wrong secret", values: env.loginForm("media-test", "wrong"), wantStatus: http.StatusUnauthorized, wantError: "invalid_client"},
		{name: "unknown client", values: env.loginForm("ghost", "x"), wantStatus: http.StatusUnauthorized, wantError: "invalid_client"},
		{name: "unsupported grant", values: url.Values{"grant_type": {"password"}}, wantStatus: http.StatusBadRequest, wantError: "unsupported_grant_type"},
		{name: "missing grant", values: url.Values{}, wantStatus: http.StatusBadRequest, wantError: "unsupported_grant_type"},
		{name: "unknown refresh token", values: url.Values{"grant_type": {"refresh_token"}, "refresh_token": {"nope"}}, wantStatus: http.StatusUnauthorized, wantError: "invalid_grant"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := env.postToken(t, tt.values, "198.51.100.1:1000")
			if rec.Code != tt.wantStatus || decodeJSON(t, rec)["error"] != tt.wantError {
				t.Fatalf("got %d %s, want %d %s", rec.Code, rec.Body.String(), tt.wantStatus, tt.wantError)
			}
			if strings.Contains(rec.Body.String(), "access_token") {
				t.Fatal("a rejected request leaked a token")
			}
		})
	}
}

func TestTokenEndpointRejectsOversizedBody(t *testing.T) {
	env := newHTTPEnv(t)
	huge := url.Values{"grant_type": {"client_credentials"}, "client_id": {strings.Repeat("a", 10000)}}

	rec := env.postToken(t, huge, "")
	if rec.Code != http.StatusBadRequest || decodeJSON(t, rec)["error"] != "invalid_request" {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
}

func TestRefreshOverHTTPRotatesAndDetectsReuse(t *testing.T) {
	env := newHTTPEnv(t)
	first := decodeJSON(t, env.postToken(t, env.loginForm("field-test", env.secrets["field-test"]), ""))
	refresh := func(token string) *httptest.ResponseRecorder {
		return env.postToken(t, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {token}}, "")
	}

	second := refresh(first["refresh_token"].(string))
	if second.Code != http.StatusOK {
		t.Fatalf("refresh: %d %s", second.Code, second.Body.String())
	}
	secondBody := decodeJSON(t, second)
	if secondBody["access_token"] == first["access_token"] || secondBody["refresh_token"] == first["refresh_token"] {
		t.Fatal("refresh must rotate both tokens")
	}

	reuse := refresh(first["refresh_token"].(string))
	if reuse.Code != http.StatusUnauthorized || decodeJSON(t, reuse)["error"] != "invalid_grant" {
		t.Fatalf("reuse: %d %s", reuse.Code, reuse.Body.String())
	}
	if !strings.Contains(env.logs.String(), "refresh token reuse") {
		t.Fatal("reuse of a refresh token must be logged as a security event")
	}
	if again := refresh(secondBody["refresh_token"].(string)); again.Code != http.StatusUnauthorized {
		t.Fatalf("the session must be dead after reuse, got %d", again.Code)
	}
}

func TestLoginIsThrottledPerPeerAfterRepeatedFailures(t *testing.T) {
	env := newHTTPEnv(t)
	attacker := "203.0.113.9:4000"

	for i := 0; i < maxFailedLogins; i++ {
		if rec := env.postToken(t, env.loginForm("media-test", "wrong"), attacker); rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: status = %d, want 401", i+1, rec.Code)
		}
	}

	blocked := env.postToken(t, env.loginForm("media-test", "wrong"), attacker)
	if blocked.Code != http.StatusTooManyRequests || blocked.Header().Get("Retry-After") != "60" || decodeJSON(t, blocked)["error"] != "too_many_attempts" {
		t.Fatalf("expected a 429 with Retry-After, got %d %v %s", blocked.Code, blocked.Header(), blocked.Body.String())
	}
	if rec := env.postToken(t, env.loginForm("media-test", env.secrets["media-test"]), attacker); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("a throttled peer stays blocked even with valid credentials, got %d", rec.Code)
	}
	if rec := env.postToken(t, env.loginForm("media-test", env.secrets["media-test"]), "198.51.100.20:1"); rec.Code != http.StatusOK {
		t.Fatalf("another peer must be unaffected, got %d", rec.Code)
	}
}

func TestSuccessfulLoginResetsTheFailureCount(t *testing.T) {
	env := newHTTPEnv(t)
	peer := "203.0.113.50:1"

	for i := 0; i < maxFailedLogins-1; i++ {
		env.postToken(t, env.loginForm("media-test", "wrong"), peer)
	}
	if rec := env.postToken(t, env.loginForm("media-test", env.secrets["media-test"]), peer); rec.Code != http.StatusOK {
		t.Fatalf("login below the limit must succeed, got %d", rec.Code)
	}
	for i := 0; i < maxFailedLogins-1; i++ {
		if rec := env.postToken(t, env.loginForm("media-test", "wrong"), peer); rec.Code != http.StatusUnauthorized {
			t.Fatalf("after a successful login the counter restarts, got %d", rec.Code)
		}
	}
}

func TestPublicKeyEndpointExposesOnlyThePublicKey(t *testing.T) {
	env := newHTTPEnv(t)
	rec := httptest.NewRecorder()
	env.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/public-key", nil))

	body := decodeJSON(t, rec)
	raw, err := base64.StdEncoding.DecodeString(body["public_key"].(string))
	if err != nil || len(raw) != ed25519.PublicKeySize || !bytes.Equal(raw, env.svc.PublicKey()) {
		t.Fatalf("public key does not match the signer: %v", body)
	}
	if body["alg"] != "EdDSA" {
		t.Fatalf("alg = %v", body["alg"])
	}
	if len(rec.Body.Bytes()) > 200 {
		t.Fatalf("response is suspiciously large, it must not carry private material: %d bytes", len(rec.Body.Bytes()))
	}
}

func TestRevocationsEndpointRequiresTheInternalKey(t *testing.T) {
	env := newHTTPEnv(t)
	first := decodeJSON(t, env.postToken(t, env.loginForm("field-test", env.secrets["field-test"]), ""))
	claims := env.parseAccess(t, first["access_token"].(string))
	env.postToken(t, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {first["refresh_token"].(string)}}, "")

	get := func(key string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/revocations", nil)
		if key != "" {
			req.Header.Set("X-Internal-Key", key)
		}
		rec := httptest.NewRecorder()
		env.handler.ServeHTTP(rec, req)
		return rec
	}

	for _, key := range []string{"", "wrong-key"} {
		if rec := get(key); rec.Code != http.StatusUnauthorized || strings.Contains(rec.Body.String(), claims.ID) {
			t.Fatalf("key %q: status %d must be 401 without leaking revocations", key, rec.Code)
		}
	}

	rec := get(testInternalKey)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body struct {
		Revoked []Revoked `json:"revoked"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Revoked) != 1 || body.Revoked[0].JTI != claims.ID || body.Revoked[0].Exp == 0 {
		t.Fatalf("revoked = %#v, want the replaced access token %s", body.Revoked, claims.ID)
	}
}

func TestLoggingMiddlewareHandlesCorrelationIDs(t *testing.T) {
	env := newHTTPEnv(t)

	send := func(id string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		if id != "" {
			req.Header.Set("X-Correlation-ID", id)
		}
		rec := httptest.NewRecorder()
		env.handler.ServeHTTP(rec, req)
		return rec
	}

	if got := send("trace-123.ok_1").Header().Get("X-Correlation-ID"); got != "trace-123.ok_1" {
		t.Fatalf("a valid id must be echoed, got %q", got)
	}
	for _, bad := range []string{"has space", "new\nline", strings.Repeat("a", 65), "semi;colon"} {
		got := send(bad).Header().Get("X-Correlation-ID")
		if got == bad {
			t.Fatalf("an unsafe correlation id %q must be replaced", bad)
		}
		if _, err := hex.DecodeString(got); err != nil || len(got) != 16 {
			t.Fatalf("replacement id %q should be 16 hex characters", got)
		}
	}
	if got := send("").Header().Get("X-Correlation-ID"); len(got) != 16 {
		t.Fatalf("a missing id must be generated, got %q", got)
	}
	for _, want := range []string{`"correlation_id":"trace-123.ok_1"`, `"duration_ms"`, `"path":"/health"`} {
		if !strings.Contains(env.logs.String(), want) {
			t.Fatalf("log lacks %s", want)
		}
	}
}

func TestValidCorrIDAndRemoteHost(t *testing.T) {
	valid := map[string]bool{
		"": false, "abc": true, "A-b_c.9": true, "a b": false, "é": false,
		strings.Repeat("x", 64): true, strings.Repeat("x", 65): false,
	}
	for in, want := range valid {
		if got := validCorrID(in); got != want {
			t.Errorf("validCorrID(%q) = %v, want %v", in, got, want)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "192.0.2.7:5555"
	if got := remoteHost(req); got != "192.0.2.7" {
		t.Errorf("remoteHost = %q", got)
	}
	req.RemoteAddr = "no-port"
	if got := remoteHost(req); got != "no-port" {
		t.Errorf("remoteHost without a port = %q", got)
	}
}
