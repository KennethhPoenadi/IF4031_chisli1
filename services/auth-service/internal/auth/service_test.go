package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"auth-service/internal/config"
)

const (
	testIssuer     = "bnpb-auth-test"
	testAccessTTL  = 60 * time.Second
	testRefreshTTL = 10 * time.Minute
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type testEnv struct {
	svc     *Service
	clock   *fakeClock
	secrets map[string]string // client id -> secret
}

func randomHex(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func newTestService(t *testing.T) *testEnv {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clients := []config.Client{
		{ID: "media-test", Secret: randomHex(t), Scope: "read:summary"},
		{ID: "field-test", Secret: randomHex(t), Scope: "read:summary read:raw"},
		{ID: "ops-test", Secret: randomHex(t), Scope: "read:summary read:raw"},
	}
	secrets := map[string]string{}
	for _, c := range clients {
		secrets[c.ID] = c.Secret
	}

	svc := NewService(config.Config{
		Issuer:     testIssuer,
		SigningKey: priv,
		AccessTTL:  testAccessTTL,
		RefreshTTL: testRefreshTTL,
		Clients:    clients,
	})
	clock := &fakeClock{t: time.Now().UTC().Truncate(time.Second)}
	svc.now = clock.Now
	return &testEnv{svc: svc, clock: clock, secrets: secrets}
}

func (e *testEnv) login(t *testing.T, clientID string) TokenPair {
	t.Helper()
	pair, err := e.svc.Login(clientID, e.secrets[clientID])
	if err != nil {
		t.Fatalf("Login(%s) error = %v", clientID, err)
	}
	return pair
}

func (e *testEnv) parseAccess(t *testing.T, token string) *Claims {
	t.Helper()
	claims := &Claims{}
	_, err := jwt.ParseWithClaims(token, claims,
		func(*jwt.Token) (any, error) { return e.svc.PublicKey(), nil },
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithIssuer(testIssuer),
		jwt.WithTimeFunc(e.clock.Now),
	)
	if err != nil {
		t.Fatalf("access token does not verify with the published public key: %v", err)
	}
	return claims
}

func (e *testEnv) revokedSet() map[string]bool {
	set := map[string]bool{}
	for _, r := range e.svc.Revocations() {
		set[r.JTI] = true
	}
	return set
}

func TestLoginIssuesSignedTokenWithServerSideScope(t *testing.T) {
	env := newTestService(t)

	tests := []struct {
		clientID string
		scope    string
	}{
		{clientID: "media-test", scope: "read:summary"},
		{clientID: "field-test", scope: "read:summary read:raw"},
		{clientID: "ops-test", scope: "read:summary read:raw"},
	}

	for _, tt := range tests {
		t.Run(tt.clientID, func(t *testing.T) {
			pair := env.login(t, tt.clientID)
			claims := env.parseAccess(t, pair.AccessToken)

			if claims.Subject != tt.clientID {
				t.Fatalf("sub = %q, want %q", claims.Subject, tt.clientID)
			}
			if claims.Scope != tt.scope || pair.Scope != tt.scope {
				t.Fatalf("scope claim = %q, response scope = %q, want %q", claims.Scope, pair.Scope, tt.scope)
			}
			if pair.TokenType != "Bearer" {
				t.Fatalf("token_type = %q, want Bearer", pair.TokenType)
			}
			if pair.ExpiresIn != int(testAccessTTL.Seconds()) {
				t.Fatalf("expires_in = %d, want %d", pair.ExpiresIn, int(testAccessTTL.Seconds()))
			}
			if got := claims.ExpiresAt.Sub(claims.IssuedAt.Time); got != testAccessTTL {
				t.Fatalf("exp - iat = %s, want the configured access TTL %s", got, testAccessTTL)
			}
			if claims.ID == "" || claims.SessionID == "" || pair.RefreshToken == "" {
				t.Fatalf("jti, sid and refresh token must all be set: %#v", claims)
			}
		})
	}
}

func TestLoginRejectsBadCredentials(t *testing.T) {
	env := newTestService(t)

	tests := []struct {
		name     string
		clientID string
		secret   string
	}{
		{name: "unknown client", clientID: "nobody", secret: env.secrets["media-test"]},
		{name: "wrong secret", clientID: "media-test", secret: "wrong"},
		{name: "empty secret", clientID: "media-test", secret: ""},
		{name: "another client's secret", clientID: "media-test", secret: env.secrets["ops-test"]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pair, err := env.svc.Login(tt.clientID, tt.secret)
			if !errors.Is(err, ErrInvalidClient) {
				t.Fatalf("Login() error = %v, want ErrInvalidClient", err)
			}
			if pair.AccessToken != "" || pair.RefreshToken != "" {
				t.Fatalf("rejected login leaked a token: %#v", pair)
			}
		})
	}
}

func TestRefreshRotatesTokensAndRevokesPreviousAccessToken(t *testing.T) {
	env := newTestService(t)
	first := env.login(t, "field-test")
	firstClaims := env.parseAccess(t, first.AccessToken)

	second, err := env.svc.Refresh(first.RefreshToken)
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	secondClaims := env.parseAccess(t, second.AccessToken)

	if second.AccessToken == first.AccessToken || second.RefreshToken == first.RefreshToken {
		t.Fatal("refresh must issue a new access token and a new refresh token")
	}
	if secondClaims.ID == firstClaims.ID {
		t.Fatal("new access token must have a new jti")
	}
	if secondClaims.SessionID != firstClaims.SessionID || secondClaims.Scope != firstClaims.Scope || secondClaims.Subject != firstClaims.Subject {
		t.Fatalf("refresh must keep the session identity, got %#v vs %#v", secondClaims, firstClaims)
	}

	revoked := env.revokedSet()
	if !revoked[firstClaims.ID] {
		t.Fatal("the previous access token must be revoked after refresh")
	}
	if revoked[secondClaims.ID] {
		t.Fatal("the newly issued access token must not be revoked")
	}
}

func TestRefreshTokenReuseRevokesWholeSession(t *testing.T) {
	env := newTestService(t)
	first := env.login(t, "field-test")

	second, err := env.svc.Refresh(first.RefreshToken)
	if err != nil {
		t.Fatalf("first Refresh() error = %v", err)
	}
	secondClaims := env.parseAccess(t, second.AccessToken)

	if _, err := env.svc.Refresh(first.RefreshToken); !errors.Is(err, ErrTokenReuse) {
		t.Fatalf("reusing a refresh token: error = %v, want ErrTokenReuse", err)
	}
	if !env.revokedSet()[secondClaims.ID] {
		t.Fatal("reuse detection must revoke the newest access token of the session")
	}
	if _, err := env.svc.Refresh(second.RefreshToken); !errors.Is(err, ErrInvalidGrant) {
		t.Fatalf("legitimate refresh token after reuse: error = %v, want ErrInvalidGrant", err)
	}
}

func TestRefreshRejectsUnknownAndExpiredTokens(t *testing.T) {
	env := newTestService(t)

	if _, err := env.svc.Refresh("not-a-real-token"); !errors.Is(err, ErrInvalidGrant) {
		t.Fatalf("unknown refresh token: error = %v, want ErrInvalidGrant", err)
	}

	pair := env.login(t, "media-test")
	env.clock.Advance(testRefreshTTL + time.Second)
	if _, err := env.svc.Refresh(pair.RefreshToken); !errors.Is(err, ErrInvalidGrant) {
		t.Fatalf("expired refresh token: error = %v, want ErrInvalidGrant", err)
	}
}

func TestRefreshWorksAfterAccessTokenExpired(t *testing.T) {
	env := newTestService(t)
	first := env.login(t, "field-test")

	env.clock.Advance(testAccessTTL + time.Second)

	second, err := env.svc.Refresh(first.RefreshToken)
	if err != nil {
		t.Fatalf("Refresh() after access token expiry error = %v", err)
	}
	claims := env.parseAccess(t, second.AccessToken)
	if !claims.ExpiresAt.After(env.clock.Now()) {
		t.Fatal("refreshed access token must be valid for a fresh TTL")
	}
	if got := len(env.svc.Revocations()); got != 0 {
		t.Fatalf("an already expired access token needs no revocation entry, got %d", got)
	}
}

func TestRevocationsDropEntriesOnceTheTokenExpires(t *testing.T) {
	env := newTestService(t)
	first := env.login(t, "field-test")
	if _, err := env.svc.Refresh(first.RefreshToken); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if got := len(env.svc.Revocations()); got != 1 {
		t.Fatalf("revocations = %d, want 1", got)
	}

	env.clock.Advance(testAccessTTL + time.Second)
	if got := len(env.svc.Revocations()); got != 0 {
		t.Fatalf("revocation list must not grow forever, got %d entries after expiry", got)
	}
}

func TestSweepRemovesExpiredSessionsAndRefreshTokens(t *testing.T) {
	env := newTestService(t)
	pair := env.login(t, "media-test")

	env.clock.Advance(testRefreshTTL + time.Second)
	env.svc.Sweep()

	env.svc.mu.Lock()
	sessions, refresh := len(env.svc.sessions), len(env.svc.refresh)
	env.svc.mu.Unlock()
	if sessions != 0 || refresh != 0 {
		t.Fatalf("after sweep sessions=%d refresh=%d, want both 0", sessions, refresh)
	}
	if _, err := env.svc.Refresh(pair.RefreshToken); !errors.Is(err, ErrInvalidGrant) {
		t.Fatalf("swept refresh token: error = %v, want ErrInvalidGrant", err)
	}
}

func TestConcurrentRefreshWithSameTokenSucceedsOnce(t *testing.T) {
	env := newTestService(t)
	pair := env.login(t, "field-test")

	const attempts = 20
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
	)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := env.svc.Refresh(pair.RefreshToken)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ErrTokenReuse), errors.Is(err, ErrInvalidGrant):
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("a single-use refresh token succeeded %d times, want exactly 1", successes)
	}
}
