package pvmbg

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port               int
	Token              string
	DelayMin           time.Duration
	DelayMax           time.Duration
	SeedCount          int
	GenerationInterval time.Duration
	RandomSeed         int64
}

func ConfigFromEnv() (Config, error) {
	port, err := intEnv("PVMBG_PORT", 8082)
	if err != nil {
		return Config{}, err
	}
	if port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("PVMBG_PORT must be between 1 and 65535")
	}
	delayMinMS, err := intEnv("PVMBG_DELAY_MIN_MS", 500)
	if err != nil {
		return Config{}, err
	}
	delayMaxMS, err := intEnv("PVMBG_DELAY_MAX_MS", 3000)
	if err != nil {
		return Config{}, err
	}
	if delayMinMS < 500 || delayMaxMS > 3000 || delayMaxMS < delayMinMS {
		return Config{}, fmt.Errorf("PVMBG delay must satisfy 500 <= min <= max <= 3000")
	}
	seedCount, err := intEnv("PVMBG_SEED_COUNT", 20)
	if err != nil {
		return Config{}, err
	}
	if seedCount < 20 {
		return Config{}, fmt.Errorf("PVMBG_SEED_COUNT must be at least 20")
	}
	intervalSeconds, err := intEnv("PVMBG_GENERATION_INTERVAL_SECONDS", 10)
	if err != nil {
		return Config{}, err
	}
	if intervalSeconds < 1 {
		return Config{}, fmt.Errorf("PVMBG_GENERATION_INTERVAL_SECONDS must be positive")
	}
	randomSeed, err := int64Env("PVMBG_RANDOM_SEED", 4031)
	if err != nil {
		return Config{}, err
	}
	token := os.Getenv("PVMBG_TOKEN")
	if token == "" {
		return Config{}, fmt.Errorf("PVMBG_TOKEN is required")
	}

	return Config{
		Port:               port,
		Token:              token,
		DelayMin:           time.Duration(delayMinMS) * time.Millisecond,
		DelayMax:           time.Duration(delayMaxMS) * time.Millisecond,
		SeedCount:          seedCount,
		GenerationInterval: time.Duration(intervalSeconds) * time.Second,
		RandomSeed:         randomSeed,
	}, nil
}

func intEnv(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return parsed, nil
}

func int64Env(name string, fallback int64) (int64, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return parsed, nil
}
