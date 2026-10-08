package authn

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const testIssuer = "bnpb-auth-test"

func newKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

func validClaims() Claims {
	now := time.Now()
	return Claims{
		Scope:     "read:summary read:raw",
		SessionID: "sid-1",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    testIssuer,
			Subject:   "field-test",
			ID:        "jti-1",
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
		},
	}
}

func mint(t *testing.T, priv ed25519.PrivateKey, mutate func(*Claims)) string {
	t.Helper()
	claims := validClaims()
	if mutate != nil {
		mutate(&claims)
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(priv)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestVerifyAcceptsValidToken(t *testing.T) {
	pub, priv := newKeyPair(t)
	v := NewVerifier(pub, testIssuer)

	claims, err := v.Verify(mint(t, priv, nil))
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if claims.Subject != "field-test" || claims.ID != "jti-1" || claims.SessionID != "sid-1" {
		t.Fatalf("unexpected claims: %#v", claims)
	}
}

func TestVerifyRejectsInvalidTokens(t *testing.T) {
	pub, priv := newKeyPair(t)
	_, otherPriv := newKeyPair(t)
	v := NewVerifier(pub, testIssuer)

	hs256, err := jwt.NewWithClaims(jwt.SigningMethodHS256, validClaims()).SignedString([]byte(pub))
	if err != nil {
		t.Fatal(err)
	}
	none, err := jwt.NewWithClaims(jwt.SigningMethodNone, validClaims()).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		token string
		want  error
	}{
		{name: "expired", token: mint(t, priv, func(c *Claims) {
			c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute))
			c.IssuedAt = jwt.NewNumericDate(time.Now().Add(-2 * time.Minute))
		}), want: ErrExpired},
		{name: "signed by another key", token: mint(t, otherPriv, nil), want: ErrInvalid},
		{name: "wrong issuer", token: mint(t, priv, func(c *Claims) { c.Issuer = "someone-else" }), want: ErrInvalid},
		{name: "no expiry", token: mint(t, priv, func(c *Claims) { c.ExpiresAt = nil }), want: ErrInvalid},
		{name: "no jti", token: mint(t, priv, func(c *Claims) { c.ID = "" }), want: ErrInvalid},
		{name: "no subject", token: mint(t, priv, func(c *Claims) { c.Subject = "" }), want: ErrInvalid},
		{name: "HS256 signed with the public key (algorithm confusion)", token: hs256, want: ErrInvalid},
		{name: "alg none", token: none, want: ErrInvalid},
		{name: "garbage", token: "not.a.jwt", want: ErrInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claims, err := v.Verify(tt.token)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Verify() error = %v, want %v", err, tt.want)
			}
			if claims != nil {
				t.Fatalf("rejected token returned claims: %#v", claims)
			}
		})
	}
}

func TestVerifyRejectsRevokedToken(t *testing.T) {
	pub, priv := newKeyPair(t)
	v := NewVerifier(pub, testIssuer)
	token := mint(t, priv, nil)

	if _, err := v.Verify(token); err != nil {
		t.Fatalf("token must be valid before revocation: %v", err)
	}

	v.mu.Lock()
	v.revoked["jti-1"] = time.Now().Add(time.Minute)
	v.mu.Unlock()

	if _, err := v.Verify(token); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Verify() error = %v, want ErrRevoked", err)
	}
}

func TestHasScope(t *testing.T) {
	tests := []struct {
		name   string
		scope  string
		wanted string
		want   bool
	}{
		{name: "exact single scope", scope: "read:summary", wanted: "read:summary", want: true},
		{name: "scope inside a list", scope: "read:summary read:raw", wanted: "read:raw", want: true},
		{name: "summary only does not grant raw", scope: "read:summary", wanted: "read:raw", want: false},
		{name: "prefix of a longer scope does not match", scope: "read:rawx", wanted: "read:raw", want: false},
		{name: "empty scope grants nothing", scope: "", wanted: "read:summary", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Claims{Scope: tt.scope}
			if got := c.HasScope(tt.wanted); got != tt.want {
				t.Fatalf("HasScope(%q) with scope %q = %v, want %v", tt.wanted, tt.scope, got, tt.want)
			}
		})
	}
}

func TestRequireEnforcesAuthenticationAndScope(t *testing.T) {
	pub, priv := newKeyPair(t)
	v := NewVerifier(pub, testIssuer)

	valid := mint(t, priv, nil)
	summaryOnly := mint(t, priv, func(c *Claims) { c.Scope = "read:summary"; c.ID = "jti-2" })
	expired := mint(t, priv, func(c *Claims) {
		c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute))
		c.IssuedAt = jwt.NewNumericDate(time.Now().Add(-2 * time.Minute))
		c.ID = "jti-3"
	})
	revoked := mint(t, priv, func(c *Claims) { c.ID = "jti-4" })
	v.mu.Lock()
	v.revoked["jti-4"] = time.Now().Add(time.Minute)
	v.mu.Unlock()

	tests := []struct {
		name       string
		header     string
		scope      string
		wantStatus int
		wantCode   string
		wantCalled bool
	}{
		{name: "no header", header: "", scope: "read:summary", wantStatus: http.StatusUnauthorized, wantCode: "missing_token"},
		{name: "not a bearer scheme", header: "Basic abc123", scope: "read:summary", wantStatus: http.StatusUnauthorized, wantCode: "missing_token"},
		{name: "garbage token", header: "Bearer garbage", scope: "read:summary", wantStatus: http.StatusUnauthorized, wantCode: "invalid_token"},
		{name: "expired token", header: "Bearer " + expired, scope: "read:summary", wantStatus: http.StatusUnauthorized, wantCode: "token_expired"},
		{name: "revoked token", header: "Bearer " + revoked, scope: "read:summary", wantStatus: http.StatusUnauthorized, wantCode: "token_revoked"},
		{name: "valid token without the required scope", header: "Bearer " + summaryOnly, scope: "read:raw", wantStatus: http.StatusForbidden, wantCode: "insufficient_scope"},
		{name: "valid token with the required scope", header: "Bearer " + valid, scope: "read:raw", wantStatus: http.StatusOK, wantCalled: true},
		{name: "scheme is case-insensitive", header: "bearer " + valid, scope: "read:summary", wantStatus: http.StatusOK, wantCalled: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			var gotSubject string
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				if claims := ClaimsFrom(r.Context()); claims != nil {
					gotSubject = claims.Subject
				}
				w.WriteHeader(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/hazard-events", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			rec := httptest.NewRecorder()
			v.Require(tt.scope)(next).ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if called != tt.wantCalled {
				t.Fatalf("handler called = %v, want %v", called, tt.wantCalled)
			}
			if tt.wantCalled && gotSubject != "field-test" {
				t.Fatalf("claims were not placed in the request context, subject = %q", gotSubject)
			}
			if tt.wantCode != "" {
				var body map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("error body is not JSON: %v", err)
				}
				if body["error"] != tt.wantCode {
					t.Fatalf("error code = %q, want %q", body["error"], tt.wantCode)
				}
				if tt.wantStatus == http.StatusUnauthorized && !strings.Contains(rec.Header().Get("WWW-Authenticate"), tt.wantCode) {
					t.Fatalf("WWW-Authenticate = %q, want it to carry %q", rec.Header().Get("WWW-Authenticate"), tt.wantCode)
				}
			}
		})
	}
}
