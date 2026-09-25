package config

import (
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

func Load() Config {
	return Config{
		MongoURI:              getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDBName:           getEnv("MONGO_DB_NAME", "bnpb_canonical"),
		MongoHazardCollection: getEnv("MONGO_HAZARD_COLLECTION", "hazard_events"),
		RabbitMQURL:           getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		RabbitMQExchange:      getEnv("RABBITMQ_EXCHANGE", "hazard_events"),
		RabbitMQExchangeType:  getEnv("RABBITMQ_EXCHANGE_TYPE", "topic"),
		AggregatorPort:        getEnv("AGGREGATOR_PORT", "8090"),
		AggregatorAPIKey:      getEnv("AGGREGATOR_API_KEY", ""),

		BMKGBaseURL: getEnv("BMKG_BASE_URL", "http://localhost:8081"),
		BMKGAPIKey:  getEnv("BMKG_API_KEY", ""),
		BMKGTimeout: getEnvMS("AGGREGATOR_BMKG_TIMEOUT_MS", 2000),

		PVMBGBaseURL: getEnv("PVMBG_BASE_URL", "http://localhost:8082"),
		PVMBGToken:   getEnv("PVMBG_TOKEN", ""),
		PVMBGTimeout: getEnvMS("AGGREGATOR_PVMBG_TIMEOUT_MS", 4000),

		PollInterval:    getEnvSeconds("AGGREGATOR_POLL_INTERVAL_SECONDS", 5),
		PVMBGStaleAfter: getEnvSeconds("AGGREGATOR_PVMBG_STALE_AFTER_SECONDS", 30),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvMS(key string, fallbackMS int) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return time.Duration(fallbackMS) * time.Millisecond
}

func getEnvSeconds(key string, fallbackSeconds int) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return time.Duration(fallbackSeconds) * time.Second
}
