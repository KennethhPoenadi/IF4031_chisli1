package bmkg

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port               int
	APIKey             string
	Delay              time.Duration
	SeedCount          int
	GenerationInterval time.Duration
	RandomSeed         int64
}

func ConfigFromEnv() (Config, error) {
	port, err := intEnv("BMKG_PORT", 8081)
	if err != nil {
		return Config{}, err
	}
	if port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("BMKG_PORT must be between 1 and 65535")
	}
	delayMS, err := intEnv("BMKG_DELAY_MS", 100)
	if err != nil {
		return Config{}, err
	}
	if delayMS < 50 || delayMS > 150 {
		return Config{}, fmt.Errorf("BMKG_DELAY_MS must be between 50 and 150")
	}
	seedCount, err := intEnv("BMKG_SEED_COUNT", 20)
	if err != nil {
		return Config{}, err
	}
	if seedCount < 20 {
		return Config{}, fmt.Errorf("BMKG_SEED_COUNT must be at least 20")
	}
	intervalSeconds, err := intEnv("BMKG_GENERATION_INTERVAL_SECONDS", 10)
	if err != nil {
		return Config{}, err
	}
	if intervalSeconds < 1 {
		return Config{}, fmt.Errorf("BMKG_GENERATION_INTERVAL_SECONDS must be positive")
	}
	randomSeed, err := int64Env("BMKG_RANDOM_SEED", 4031)
	if err != nil {
		return Config{}, err
	}
	apiKey := os.Getenv("BMKG_API_KEY")
	if apiKey == "" {
		return Config{}, fmt.Errorf("BMKG_API_KEY is required")
	}

	return Config{
		Port:               port,
		APIKey:             apiKey,
		Delay:              time.Duration(delayMS) * time.Millisecond,
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
