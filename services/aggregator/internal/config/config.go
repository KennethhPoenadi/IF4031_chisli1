package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	MongoURI              string
	MongoDBName           string
	MongoHazardCollection string
	RabbitMQURL           string
	RabbitMQExchange      string
	RabbitMQExchangeType  string
	AggregatorPort        string
	AggregatorAPIKey      string

	BMKGBaseURL string
	BMKGAPIKey  string
	BMKGTimeout time.Duration

	PVMBGBaseURL string
	PVMBGToken   string
	PVMBGTimeout time.Duration

	PollInterval    time.Duration
	PVMBGStaleAfter time.Duration
}

func Load() (Config, error) {
	port, err := strconv.Atoi(getEnv("AGGREGATOR_PORT", "8090"))
	if err != nil || port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("AGGREGATOR_PORT must be an integer between 1 and 65535")
	}

	aggregatorAPIKey := getEnv("AGGREGATOR_API_KEY", "")
	if aggregatorAPIKey == "" {
		return Config{}, fmt.Errorf("AGGREGATOR_API_KEY is required")
	}

	bmkgAPIKey := getEnv("BMKG_API_KEY", "")
	if bmkgAPIKey == "" {
		return Config{}, fmt.Errorf("BMKG_API_KEY is required")
	}

	pvmbgToken := getEnv("PVMBG_TOKEN", "")
	if pvmbgToken == "" {
		return Config{}, fmt.Errorf("PVMBG_TOKEN is required")
	}

	rabbitMQURL := getEnv("RABBITMQ_URL", "")
	if rabbitMQURL == "" {
		return Config{}, fmt.Errorf("RABBITMQ_URL is required")
	}

	pollInterval, err := getEnvSeconds("AGGREGATOR_POLL_INTERVAL_SECONDS", 5)
	if err != nil {
		return Config{}, err
	}

	bmkgTimeout, err := getEnvMS("AGGREGATOR_BMKG_TIMEOUT_MS", 2000)
	if err != nil {
		return Config{}, err
	}

	pvmbgTimeout, err := getEnvMS("AGGREGATOR_PVMBG_TIMEOUT_MS", 4000)
	if err != nil {
		return Config{}, err
	}

	pvmbgStaleAfter, err := getEnvSeconds("AGGREGATOR_PVMBG_STALE_AFTER_SECONDS", 30)
	if err != nil {
		return Config{}, err
	}

	return Config{
		MongoURI:              getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDBName:           getEnv("MONGO_DB_NAME", "bnpb_canonical"),
		MongoHazardCollection: getEnv("MONGO_HAZARD_COLLECTION", "hazard_events"),
		RabbitMQURL:           rabbitMQURL,
		RabbitMQExchange:      getEnv("RABBITMQ_EXCHANGE", "hazard_events"),
		RabbitMQExchangeType:  getEnv("RABBITMQ_EXCHANGE_TYPE", "topic"),
		AggregatorPort:        strconv.Itoa(port),
		AggregatorAPIKey:      aggregatorAPIKey,

		BMKGBaseURL: getEnv("BMKG_BASE_URL", "http://localhost:8081"),
		BMKGAPIKey:  bmkgAPIKey,
		BMKGTimeout: bmkgTimeout,

		PVMBGBaseURL: getEnv("PVMBG_BASE_URL", "http://localhost:8082"),
		PVMBGToken:   pvmbgToken,
		PVMBGTimeout: pvmbgTimeout,

		PollInterval:    pollInterval,
		PVMBGStaleAfter: pvmbgStaleAfter,
	}, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvMS(key string, fallbackMS int) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return time.Duration(fallbackMS) * time.Millisecond, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return time.Duration(n) * time.Millisecond, nil
}

func getEnvSeconds(key string, fallbackSeconds int) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return time.Duration(fallbackSeconds) * time.Second, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return time.Duration(n) * time.Second, nil
}
