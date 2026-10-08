package bmkg

import (
	"testing"
	"time"
)

func setEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	base := map[string]string{
		"BMKG_PORT": "", "BMKG_DELAY_MS": "", "BMKG_SEED_COUNT": "", "BMKG_GENERATION_INTERVAL_SECONDS": "",
		"BMKG_RANDOM_SEED": "", "BMKG_API_KEY": "key-for-tests",
	}
	for k, v := range overrides {
		base[k] = v
	}
	for k, v := range base {
		t.Setenv(k, v)
	}
}

func TestConfigFromEnvDefaultsMatchTheSpec(t *testing.T) {
	setEnv(t, nil)

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if cfg.Port != 8081 {
		t.Fatalf("port = %d, the spec fixes BMKG on 8081", cfg.Port)
	}
	if cfg.Delay < 50*time.Millisecond || cfg.Delay > 150*time.Millisecond {
		t.Fatalf("delay = %s, must be a fixed value inside 50-150ms", cfg.Delay)
	}
	if cfg.SeedCount < 20 || cfg.GenerationInterval > 10*time.Second {
		t.Fatalf("seed=%d interval=%s, want at least 20 records and one new event per 10s", cfg.SeedCount, cfg.GenerationInterval)
	}
	if cfg.APIKey != "key-for-tests" || cfg.RandomSeed != 4031 {
		t.Fatalf("api key / seed = %q / %d", cfg.APIKey, cfg.RandomSeed)
	}
}

func TestConfigFromEnvAcceptsInRangeOverrides(t *testing.T) {
	setEnv(t, map[string]string{
		"BMKG_PORT": "9001", "BMKG_DELAY_MS": "50", "BMKG_SEED_COUNT": "40",
		"BMKG_GENERATION_INTERVAL_SECONDS": "1", "BMKG_RANDOM_SEED": "7",
	})

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if cfg.Port != 9001 || cfg.Delay != 50*time.Millisecond || cfg.SeedCount != 40 || cfg.GenerationInterval != time.Second || cfg.RandomSeed != 7 {
		t.Fatalf("config = %#v", cfg)
	}
}

func TestConfigFromEnvEnforcesTheContract(t *testing.T) {
	tests := map[string]map[string]string{
		"missing api key":          {"BMKG_API_KEY": ""},
		"delay below 50ms":         {"BMKG_DELAY_MS": "49"},
		"delay above 150ms":        {"BMKG_DELAY_MS": "151"},
		"fewer than 20 seed rows":  {"BMKG_SEED_COUNT": "19"},
		"zero interval":            {"BMKG_GENERATION_INTERVAL_SECONDS": "0"},
		"port zero":                {"BMKG_PORT": "0"},
		"port too large":           {"BMKG_PORT": "65536"},
		"port not a number":        {"BMKG_PORT": "abc"},
		"delay not a number":       {"BMKG_DELAY_MS": "fast"},
		"seed count not a number":  {"BMKG_SEED_COUNT": "many"},
		"interval not a number":    {"BMKG_GENERATION_INTERVAL_SECONDS": "soon"},
		"random seed not a number": {"BMKG_RANDOM_SEED": "x"},
	}

	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			setEnv(t, env)
			if _, err := ConfigFromEnv(); err == nil {
				t.Fatal("ConfigFromEnv() accepted a configuration that breaks the contract")
			}
		})
	}
}
