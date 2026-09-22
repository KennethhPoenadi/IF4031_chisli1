package config

import "os"

type Config struct {
	MongoURI             string
	MongoDBName           string
	MongoHazardCollection string
	RabbitMQURL           string
	RabbitMQExchange      string
	RabbitMQExchangeType  string
	AggregatorPort        string
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
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
