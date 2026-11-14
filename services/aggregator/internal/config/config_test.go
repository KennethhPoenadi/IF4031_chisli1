package config

import (
	"crypto/rand"
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

// setValidEnv sets the required variables and clears every optional one, so a test never
// depends on the developer's shell.
func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("AGGREGATOR_API_KEY", randomHex(t))
	t.Setenv("BMKG_API_KEY", randomHex(t))
	t.Setenv("PVMBG_TOKEN", randomHex(t))
	t.Setenv("RABBITMQ_URL", "amqp://user:"+randomHex(t)+"@broker:5672/")
	for _, key := range []string{
		"MONGO_URI", "MONGO_DB_NAME", "MONGO_HAZARD_COLLECTION", "RABBITMQ_EXCHANGE", "RABBITMQ_EXCHANGE_TYPE",
		"AGGREGATOR_PORT", "BMKG_BASE_URL", "PVMBG_BASE_URL", "AGGREGATOR_POLL_INTERVAL_SECONDS",
		"AGGREGATOR_BMKG_TIMEOUT_MS", "AGGREGATOR_PVMBG_TIMEOUT_MS", "AGGREGATOR_PVMBG_STALE_AFTER_SECONDS",
		"AGGREGATOR_PANTAI_ENABLED", "PANTAI_BASE_URL", "PANTAI_AUTH_TOKEN", "AGGREGATOR_PANTAI_TIMEOUT_MS",
		"AGGREGATOR_PANTAI_POLL_INTERVAL_MS", "AGGREGATOR_PANTAI_STALE_AFTER_SECONDS",
		"AGGREGATOR_PANTAI_STATION_CACHE_TTL_SECONDS", "AGGREGATOR_PANTAI_CIRCUIT_FAILURES",
		"AGGREGATOR_PANTAI_CIRCUIT_OPEN_SECONDS",
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

	if cfg.AggregatorPort != "8090" || cfg.RabbitMQExchange != "hazard_events" || cfg.RabbitMQExchangeType != "topic" {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
	if cfg.MongoDBName != "bnpb_canonical" || cfg.MongoHazardCollection != "hazard_events" {
		t.Fatalf("unexpected storage defaults: %#v", cfg)
	}
	if cfg.PollInterval != 5*time.Second {
		t.Fatalf("poll interval = %s, want 5s (inside the suggested 2-5s range)", cfg.PollInterval)
	}
	if cfg.BMKGTimeout != 2*time.Second || cfg.PVMBGTimeout != 4*time.Second || cfg.PVMBGStaleAfter != 30*time.Second {
		t.Fatalf("timeouts = %s/%s stale=%s", cfg.BMKGTimeout, cfg.PVMBGTimeout, cfg.PVMBGStaleAfter)
	}
	if cfg.PVMBGTimeout <= 3*time.Second {
		t.Fatal("the PVMBG timeout must exceed PVMBG's maximum 3s delay")
	}
	if cfg.PantaiEnabled || cfg.PantaiBaseURL != "http://sensor-pantai:8080" || cfg.PantaiTimeout != 1500*time.Millisecond ||
		cfg.PantaiPollInterval != time.Second || cfg.PantaiStaleAfter != 30*time.Second ||
		cfg.PantaiStationCacheTTL != 5*time.Minute || cfg.PantaiCircuitFailures != 3 || cfg.PantaiCircuitOpen != 10*time.Second {
		t.Fatalf("unexpected Sensor Pantai defaults: %#v", cfg)
	}
}

func TestLoadReadsOverrides(t *testing.T) {
	setValidEnv(t)
	t.Setenv("AGGREGATOR_PORT", "9000")
	t.Setenv("AGGREGATOR_POLL_INTERVAL_SECONDS", "2")
	t.Setenv("AGGREGATOR_BMKG_TIMEOUT_MS", "750")
	t.Setenv("AGGREGATOR_PVMBG_TIMEOUT_MS", "6000")
	t.Setenv("AGGREGATOR_PVMBG_STALE_AFTER_SECONDS", "10")
	t.Setenv("BMKG_BASE_URL", "http://bmkg:1")
	t.Setenv("PVMBG_BASE_URL", "http://pvmbg:2")
	t.Setenv("MONGO_URI", "mongodb://mongo:27017")
	t.Setenv("AGGREGATOR_PANTAI_ENABLED", "true")
	t.Setenv("PANTAI_BASE_URL", "http://pantai:8080")
	t.Setenv("PANTAI_AUTH_TOKEN", "pantai-token")
	t.Setenv("AGGREGATOR_PANTAI_TIMEOUT_MS", "900")
	t.Setenv("AGGREGATOR_PANTAI_POLL_INTERVAL_MS", "750")
	t.Setenv("AGGREGATOR_PANTAI_STALE_AFTER_SECONDS", "45")
	t.Setenv("AGGREGATOR_PANTAI_STATION_CACHE_TTL_SECONDS", "120")
	t.Setenv("AGGREGATOR_PANTAI_CIRCUIT_FAILURES", "5")
	t.Setenv("AGGREGATOR_PANTAI_CIRCUIT_OPEN_SECONDS", "20")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.AggregatorPort != "9000" || cfg.PollInterval != 2*time.Second || cfg.BMKGTimeout != 750*time.Millisecond ||
		cfg.PVMBGTimeout != 6*time.Second || cfg.PVMBGStaleAfter != 10*time.Second {
		t.Fatalf("overrides not applied: %#v", cfg)
	}
	if cfg.BMKGBaseURL != "http://bmkg:1" || cfg.PVMBGBaseURL != "http://pvmbg:2" || cfg.MongoURI != "mongodb://mongo:27017" {
		t.Fatalf("URL overrides not applied: %#v", cfg)
	}
	if !cfg.PantaiEnabled || cfg.PantaiBaseURL != "http://pantai:8080" || cfg.PantaiToken != "pantai-token" ||
		cfg.PantaiTimeout != 900*time.Millisecond || cfg.PantaiPollInterval != 750*time.Millisecond ||
		cfg.PantaiStaleAfter != 45*time.Second || cfg.PantaiStationCacheTTL != 2*time.Minute ||
		cfg.PantaiCircuitFailures != 5 || cfg.PantaiCircuitOpen != 20*time.Second {
		t.Fatalf("Sensor Pantai overrides not applied: %#v", cfg)
	}
}

func TestLoadFailsFastWithoutSecrets(t *testing.T) {
	for _, key := range []string{"AGGREGATOR_API_KEY", "BMKG_API_KEY", "PVMBG_TOKEN", "RABBITMQ_URL"} {
		t.Run(key, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv(key, "")
			if _, err := Load(); err == nil {
				t.Fatalf("Load() must refuse to start without %s", key)
			}
		})
	}
}

func TestLoadRejectsInvalidNumbers(t *testing.T) {
	tests := []struct{ key, value string }{
		{"AGGREGATOR_PORT", "abc"},
		{"AGGREGATOR_PORT", "0"},
		{"AGGREGATOR_PORT", "70000"},
		{"AGGREGATOR_POLL_INTERVAL_SECONDS", "abc"},
		{"AGGREGATOR_POLL_INTERVAL_SECONDS", "0"},
		{"AGGREGATOR_POLL_INTERVAL_SECONDS", "-3"},
		{"AGGREGATOR_BMKG_TIMEOUT_MS", "abc"},
		{"AGGREGATOR_BMKG_TIMEOUT_MS", "0"},
		{"AGGREGATOR_PVMBG_TIMEOUT_MS", "-1"},
		{"AGGREGATOR_PVMBG_STALE_AFTER_SECONDS", "0"},
		{"AGGREGATOR_PANTAI_ENABLED", "sometimes"},
		{"AGGREGATOR_PANTAI_TIMEOUT_MS", "0"},
		{"AGGREGATOR_PANTAI_POLL_INTERVAL_MS", "-1"},
		{"AGGREGATOR_PANTAI_STALE_AFTER_SECONDS", "0"},
		{"AGGREGATOR_PANTAI_STATION_CACHE_TTL_SECONDS", "0"},
		{"AGGREGATOR_PANTAI_CIRCUIT_FAILURES", "0"},
		{"AGGREGATOR_PANTAI_CIRCUIT_OPEN_SECONDS", "0"},
	}

	for _, tt := range tests {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv(tt.key, tt.value)
			if _, err := Load(); err == nil {
				t.Fatalf("Load() accepted %s=%s", tt.key, tt.value)
			}
		})
	}
}

func TestLoadRequiresPantaiTokenWhenEnabled(t *testing.T) {
	setValidEnv(t)
	t.Setenv("AGGREGATOR_PANTAI_ENABLED", "true")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted enabled Sensor Pantai without PANTAI_AUTH_TOKEN")
	}
}
