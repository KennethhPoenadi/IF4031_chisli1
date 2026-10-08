package config

import (
	"fmt"
	"os"
)

type Config struct {
	RabbitURL  string
	Exchange   string
	Queue      string
	BindingKey string
	Prefetch   int
	HealthPort string
}

func Load() (Config, error) {
	rabbitURL := getEnv("RABBITMQ_URL", "")
	if rabbitURL == "" {
		return Config{}, fmt.Errorf("RABBITMQ_URL is required")
	}
	return Config{
		RabbitURL:  rabbitURL,
		Exchange:   getEnv("RABBITMQ_EXCHANGE", "hazard_events"),
		Queue:      getEnv("DASHBOARD_UPDATER_QUEUE", "dashboard-updater-queue"),
		BindingKey: getEnv("BINDING_KEY", "hazard.#"),
		Prefetch:   getInt("DASHBOARD_UPDATER_PREFETCH", 10),
		HealthPort: getEnv("DASHBOARD_UPDATER_HEALTH_PORT", "8092"),
	}, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n >= 0 {
			return n
		}
	}
	return fallback
}
