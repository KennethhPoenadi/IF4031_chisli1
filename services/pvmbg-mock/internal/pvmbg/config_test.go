package pvmbg

import (
	"testing"
	"time"
)

func setEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	base := map[string]string{
		"PVMBG_PORT": "", "PVMBG_DELAY_MIN_MS": "", "PVMBG_DELAY_MAX_MS": "", "PVMBG_SEED_COUNT": "",
		"PVMBG_GENERATION_INTERVAL_SECONDS": "", "PVMBG_RANDOM_SEED": "", "PVMBG_TOKEN": "token-for-tests",
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
	if cfg.Port != 8082 {
		t.Fatalf("port = %d, the spec fixes PVMBG on 8082", cfg.Port)
	}
	if cfg.DelayMin != 500*time.Millisecond || cfg.DelayMax != 3*time.Second {
		t.Fatalf("delay range = %s-%s, the spec says 500ms-3s", cfg.DelayMin, cfg.DelayMax)
	}
	if cfg.SeedCount < 20 || cfg.GenerationInterval > 10*time.Second || cfg.Token != "token-for-tests" || cfg.RandomSeed != 4031 {
		t.Fatalf("config = %#v", cfg)
	}
}

func TestConfigFromEnvAllowsAFixedThreeSecondDelayForTheP2Test(t *testing.T) {
	setEnv(t, map[string]string{"PVMBG_DELAY_MIN_MS": "3000", "PVMBG_DELAY_MAX_MS": "3000", "PVMBG_PORT": "9002", "PVMBG_SEED_COUNT": "30"})

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if cfg.DelayMin != 3*time.Second || cfg.DelayMax != 3*time.Second || cfg.Port != 9002 || cfg.SeedCount != 30 {
		t.Fatalf("config = %#v", cfg)
	}
}

func TestConfigFromEnvEnforcesTheContract(t *testing.T) {
	tests := map[string]map[string]string{
		"missing token":             {"PVMBG_TOKEN": ""},
		"minimum delay below 500ms": {"PVMBG_DELAY_MIN_MS": "499"},
		"maximum delay above 3s":    {"PVMBG_DELAY_MAX_MS": "3001"},
		"maximum below minimum":     {"PVMBG_DELAY_MIN_MS": "2000", "PVMBG_DELAY_MAX_MS": "1000"},
		"fewer than 20 seed rows":   {"PVMBG_SEED_COUNT": "5"},
		"zero interval":             {"PVMBG_GENERATION_INTERVAL_SECONDS": "0"},
		"port zero":                 {"PVMBG_PORT": "0"},
		"port too large":            {"PVMBG_PORT": "70000"},
		"port not a number":         {"PVMBG_PORT": "abc"},
		"min delay not a number":    {"PVMBG_DELAY_MIN_MS": "slow"},
		"max delay not a number":    {"PVMBG_DELAY_MAX_MS": "slow"},
		"seed count not a number":   {"PVMBG_SEED_COUNT": "many"},
		"interval not a number":     {"PVMBG_GENERATION_INTERVAL_SECONDS": "soon"},
		"random seed not a number":  {"PVMBG_RANDOM_SEED": "x"},
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
