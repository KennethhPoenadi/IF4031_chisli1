package config

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

func randomHex(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// setValidEnv fills every required variable with generated values and clears the optional ones,
// so each test starts from a known state regardless of the developer's shell.
func setValidEnv(t *testing.T) {
	t.Helper()
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AUTH_SIGNING_SEED", base64.StdEncoding.EncodeToString(seed))
	t.Setenv("AUTH_INTERNAL_KEY", randomHex(t))
	for _, prefix := range []string{"AUTH_CLIENT_MEDIA", "AUTH_CLIENT_FIELD", "AUTH_CLIENT_OPS"} {
		t.Setenv(prefix+"_ID", prefix+"-id")
		t.Setenv(prefix+"_SECRET", randomHex(t))
	}
	for _, key := range []string{"AUTH_PORT", "AUTH_TOKEN_ISSUER", "AUTH_ACCESS_TTL_SECONDS", "AUTH_REFRESH_TTL_SECONDS"} {
		t.Setenv(key, "")
	}
}

func TestLoadAssignsScopesOnTheServerSide(t *testing.T) {
	setValidEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.Clients) != 3 {
		t.Fatalf("clients = %d, want 3", len(cfg.Clients))
	}

	scopes := map[string]string{}
	for _, c := range cfg.Clients {
		scopes[c.ID] = c.Scope
	}
	if got := scopes["AUTH_CLIENT_MEDIA-id"]; got != "read:summary" {
		t.Fatalf("media scope = %q, want only read:summary", got)
	}
	for _, id := range []string{"AUTH_CLIENT_FIELD-id", "AUTH_CLIENT_OPS-id"} {
		if !strings.Contains(scopes[id], "read:raw") {
			t.Fatalf("%s scope = %q, want it to include read:raw", id, scopes[id])
		}
	}
}

func TestLoadDefaultsAndOverridesTTL(t *testing.T) {
	setValidEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.AccessTTL != 60*time.Second {
		t.Fatalf("default access TTL = %s, want 60s as required by the spec", cfg.AccessTTL)
	}

	t.Setenv("AUTH_ACCESS_TTL_SECONDS", "15")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.AccessTTL != 15*time.Second {
		t.Fatalf("configured access TTL = %s, want 15s", cfg.AccessTTL)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T)
	}{
		{name: "missing signing seed", mutate: func(t *testing.T) { t.Setenv("AUTH_SIGNING_SEED", "") }},
		{name: "signing seed has wrong length", mutate: func(t *testing.T) {
			t.Setenv("AUTH_SIGNING_SEED", base64.StdEncoding.EncodeToString([]byte("too-short")))
		}},
		{name: "signing seed is not base64", mutate: func(t *testing.T) { t.Setenv("AUTH_SIGNING_SEED", "%%%not-base64%%%") }},
		{name: "missing internal key", mutate: func(t *testing.T) { t.Setenv("AUTH_INTERNAL_KEY", "") }},
		{name: "missing client secret", mutate: func(t *testing.T) { t.Setenv("AUTH_CLIENT_OPS_SECRET", "") }},
		{name: "missing client id", mutate: func(t *testing.T) { t.Setenv("AUTH_CLIENT_FIELD_ID", "") }},
		{name: "two clients share an id", mutate: func(t *testing.T) { t.Setenv("AUTH_CLIENT_OPS_ID", "AUTH_CLIENT_MEDIA-id") }},
		{name: "two clients share a secret", mutate: func(t *testing.T) {
			t.Setenv("AUTH_CLIENT_OPS_SECRET", "same-secret")
			t.Setenv("AUTH_CLIENT_FIELD_SECRET", "same-secret")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setValidEnv(t)
			tt.mutate(t)
			if _, err := Load(); err == nil {
				t.Fatal("Load() accepted an invalid configuration")
			}
		})
	}
}

func TestLoadReadsPortAndIssuerOverrides(t *testing.T) {
	setValidEnv(t)
	t.Setenv("AUTH_PORT", "9191")
	t.Setenv("AUTH_TOKEN_ISSUER", "custom-issuer")
	t.Setenv("AUTH_REFRESH_TTL_SECONDS", "120")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Port != "9191" || cfg.Issuer != "custom-issuer" || cfg.RefreshTTL != 120*time.Second {
		t.Fatalf("overrides not applied: port=%q issuer=%q refresh=%s", cfg.Port, cfg.Issuer, cfg.RefreshTTL)
	}
}

// An unusable TTL falls back to the default rather than producing a token that never or instantly expires.
func TestLoadFallsBackToDefaultTTLForUnusableValues(t *testing.T) {
	for _, value := range []string{"abc", "0", "-5"} {
		t.Run(value, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("AUTH_ACCESS_TTL_SECONDS", value)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.AccessTTL != 60*time.Second {
				t.Fatalf("access TTL = %s, want the 60s default", cfg.AccessTTL)
			}
		})
	}
}
