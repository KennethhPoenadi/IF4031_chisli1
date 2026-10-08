package authn

import (
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"client-facing-api/internal/httpx"
)

var (
	ErrExpired = errors.New("token expired")
	ErrRevoked = errors.New("token revoked")
	ErrInvalid = errors.New("token invalid")
)

type Claims struct {
	Scope     string `json:"scope"`
	SessionID string `json:"sid"`
	jwt.RegisteredClaims
}

func (c *Claims) HasScope(scope string) bool {
	for _, s := range strings.Fields(c.Scope) {
		if subtle.ConstantTimeCompare([]byte(s), []byte(scope)) == 1 {
			return true
		}
	}
	return false
}

type Verifier struct {
	pub    ed25519.PublicKey
	issuer string

	mu      sync.RWMutex
	revoked map[string]time.Time // jti -> exp
}

func NewVerifier(pub ed25519.PublicKey, issuer string) *Verifier {
	return &Verifier{pub: pub, issuer: issuer, revoked: map[string]time.Time{}}
}

func (v *Verifier) Verify(raw string) (*Claims, error) {
	claims := &Claims{}
	_, err := jwt.ParseWithClaims(raw, claims,
		func(t *jwt.Token) (any, error) { return v.pub, nil },
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithIssuer(v.issuer),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpired
		}
		return nil, ErrInvalid
	}
	if claims.ID == "" || claims.Subject == "" {
		return nil, ErrInvalid
	}
	v.mu.RLock()
	_, isRevoked := v.revoked[claims.ID]
	v.mu.RUnlock()
	if isRevoked {
		return nil, ErrRevoked
	}
	return claims, nil
}

func (v *Verifier) SyncRevocations(ctx context.Context, authURL, internalKey string, every time.Duration, log *slog.Logger) {
	client := &http.Client{Timeout: 2 * time.Second}
	t := time.NewTicker(every)
	defer t.Stop()
	failing := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		list, err := fetchRevocations(ctx, client, authURL, internalKey)
		if err != nil {
			if !failing {
				log.Warn("revocation sync failing, keeping last known list", "err", err)
				failing = true
			}
			continue
		}
		if failing {
			log.Info("revocation sync recovered")
			failing = false
		}
		now := time.Now()
		v.mu.Lock()
		for _, r := range list {
			v.revoked[r.JTI] = time.Unix(r.Exp, 0)
		}
		for jti, exp := range v.revoked {
			if now.After(exp) {
				delete(v.revoked, jti)
			}
		}
		v.mu.Unlock()
	}
}

type revocation struct {
	JTI string `json:"jti"`
	Exp int64  `json:"exp"`
}

func fetchRevocations(ctx context.Context, c *http.Client, authURL, key string) ([]revocation, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, authURL+"/revocations", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Internal-Key", key)
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("auth-service returned %d", resp.StatusCode)
	}
	var body struct {
		Revoked []revocation `json:"revoked"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return nil, err
	}
	return body.Revoked, nil
}

// publicKeyRetryDelay is the pause between attempts to fetch the auth-service public key at startup.
var publicKeyRetryDelay = 2 * time.Second

func FetchPublicKey(ctx context.Context, authURL string, attempts int, log *slog.Logger) (ed25519.PublicKey, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	var lastErr error
	for i := 1; i <= attempts; i++ {
		key, err := fetchOnce(ctx, client, authURL)
		if err == nil {
			return key, nil
		}
		lastErr = err
		log.Warn("waiting for auth-service public key", "attempt", i, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(publicKeyRetryDelay):
		}
	}
	return nil, lastErr
}

func fetchOnce(ctx context.Context, c *http.Client, authURL string) (ed25519.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, authURL+"/public-key", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("auth-service returned %d", resp.StatusCode)
	}
	var body struct {
		Alg       string `json:"alg"`
		PublicKey string `json:"public_key"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&body); err != nil {
		return nil, err
	}
	raw, err := base64.StdEncoding.DecodeString(body.PublicKey)
	if err != nil || body.Alg != "EdDSA" || len(raw) != ed25519.PublicKeySize {
		return nil, errors.New("auth-service returned an unusable public key")
	}
	return ed25519.PublicKey(raw), nil
}

func (v *Verifier) Require(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := r.Header.Get("Authorization")
			if len(h) < 8 || !strings.EqualFold(h[:7], "bearer ") {
				unauthorized(w, "missing_token", "Authorization: Bearer <token> is required")
				return
			}
			claims, err := v.Verify(strings.TrimSpace(h[7:]))
			if err != nil {
				switch {
				case errors.Is(err, ErrExpired):
					unauthorized(w, "token_expired", "access token expired, use the refresh token")
				case errors.Is(err, ErrRevoked):
					unauthorized(w, "token_revoked", "access token was revoked (replaced by a refresh)")
				default:
					unauthorized(w, "invalid_token", "access token is not valid")
				}
				return
			}
			httpx.SetClient(r.Context(), claims.Subject)
			if !claims.HasScope(scope) {
				httpx.WriteError(w, http.StatusForbidden, "insufficient_scope", "this client is not allowed to call this endpoint")
				return
			}
			next.ServeHTTP(w, r.WithContext(WithClaims(r.Context(), claims)))
		})
	}
}

func unauthorized(w http.ResponseWriter, code, msg string) {
	w.Header().Set("WWW-Authenticate", `Bearer error="`+code+`"`)
	httpx.WriteError(w, http.StatusUnauthorized, code, msg)
}

type claimsKey struct{}

func WithClaims(ctx context.Context, c *Claims) context.Context {
	return context.WithValue(ctx, claimsKey{}, c)
}

func ClaimsFrom(ctx context.Context) *Claims {
	c, _ := ctx.Value(claimsKey{}).(*Claims)
	return c
}
