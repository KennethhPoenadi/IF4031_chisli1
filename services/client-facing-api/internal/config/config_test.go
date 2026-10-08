package config

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"
)

func randomHex(t *testing.T) string {
	t.Helper()
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("AGGREGATOR_API_KEY", randomHex(t))
	t.Setenv("AUTH_INTERNAL_KEY", randomHex(t))
	for _, key := range []string{
		"CLIENT_API_PORT", "AUTH_TOKEN_ISSUER", "AGGREGATOR_URL", "CLIENT_API_AGGREGATOR_TIMEOUT_MS",
		"AUTH_SERVICE_URL", "CLIENT_API_REVOCATION_POLL_MS", "CLIENT_API_MAX_INFLIGHT", "AUTH_PUBLIC_KEY",
	} {
		t.Setenv(key, "")
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	setValidEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Port != "8080" {
		t.Fatalf("port = %q, the spec says the client-facing API listens on 8080", cfg.Port)
	}
	if cfg.AggregatorURL != "http://aggregator:8090" || cfg.AuthURL != "http://auth-service:8091" || cfg.Issuer != "bnpb-auth" {
		t.Fatalf("unexpected URL/issuer defaults: %#v", cfg)
	}
	if cfg.AggregatorTimeout != 1500*time.Millisecond || cfg.RevocationPoll != 250*time.Millisecond || cfg.MaxInflight != 200 {
		t.Fatalf("timeouts/limits: %s %s %d", cfg.AggregatorTimeout, cfg.RevocationPoll, cfg.MaxInflight)
	}
	if cfg.PinnedPublicKey != nil {
		t.Fatal("no public key is pinned unless configured")
	}
}

func TestLoadReadsOverridesAndFallsBackOnUnusableNumbers(t *testing.T) {
	setValidEnv(t)
	t.Setenv("CLIENT_API_PORT", "9090")
	t.Setenv("CLIENT_API_MAX_INFLIGHT", "75")
	t.Setenv("CLIENT_API_AGGREGATOR_TIMEOUT_MS", "800")
	t.Setenv("CLIENT_API_REVOCATION_POLL_MS", "abc")
	t.Setenv("AGGREGATOR_URL", "http://agg:1")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Port != "9090" || cfg.MaxInflight != 75 || cfg.AggregatorTimeout != 800*time.Millisecond || cfg.AggregatorURL != "http://agg:1" {
		t.Fatalf("overrides not applied: %#v", cfg)
	}
	if cfg.RevocationPoll != 250*time.Millisecond {
		t.Fatalf("an unusable value must fall back to the default, got %s", cfg.RevocationPoll)
	}

	t.Setenv("CLIENT_API_MAX_INFLIGHT", "0")
	if cfg, _ = Load(); cfg.MaxInflight != 200 {
		t.Fatalf("a zero limit would reject every request, expected the default, got %d", cfg.MaxInflight)
	}
}

func TestLoadRequiresBothInternalKeys(t *testing.T) {
	for _, key := range []string{"AGGREGATOR_API_KEY", "AUTH_INTERNAL_KEY"} {
		t.Run(key, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv(key, "")
			if _, err := Load(); err == nil {
				t.Fatalf("Load() must refuse to start without %s", key)
			}
		})
	}
}

func TestLoadPinnedPublicKey(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("valid key is pinned", func(t *testing.T) {
		setValidEnv(t)
		t.Setenv("AUTH_PUBLIC_KEY", base64.StdEncoding.EncodeToString(pub))
		cfg, err := Load()
		if err != nil || !cfg.PinnedPublicKey.Equal(pub) {
			t.Fatalf("pinned key = %v, err = %v", cfg.PinnedPublicKey, err)
		}
	})

	for name, value := range map[string]string{
		"not base64":   "%%%",
		"wrong length": base64.StdEncoding.EncodeToString([]byte("short")),
	} {
		t.Run(name, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("AUTH_PUBLIC_KEY", value)
			if _, err := Load(); err == nil {
				t.Fatal("an unusable pinned key must stop the service from starting")
			}
		})
	}
}
