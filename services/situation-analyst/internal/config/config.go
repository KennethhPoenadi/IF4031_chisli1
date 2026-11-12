package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port        string
	InternalKey string

	RabbitURL      string
	Exchange       string
	Queue          string
	BindingKey     string
	Prefetch       int
	QueueMaxLength int

	WindowMinutes   int
	SummaryInterval time.Duration

	LLMBaseURL    string
	LLMModel      string
	LLMAPIKey     string
	LLMTimeout    time.Duration
	LLMMaxPerHour int
}

func get(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func getInt(key string, fallback int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s harus bilangan bulat positif", key)
	}
	return n, nil
}

func Load() (Config, error) {
	c := Config{
		Port:        get("SITUATION_ANALYST_PORT", "8094"),
		InternalKey: get("ANALYST_API_KEY", ""),
		RabbitURL:   get("RABBITMQ_URL", ""),
		Exchange:    get("RABBITMQ_EXCHANGE", "hazard_events"),
		Queue:       get("ANALYST_QUEUE", "situation-analyst-queue"),
		BindingKey:  get("ANALYST_BINDING_KEY", "hazard.#"),
		LLMBaseURL:  get("LLM_BASE_URL", ""),
		LLMModel:    get("LLM_MODEL", ""),
		LLMAPIKey:   get("LLM_API_KEY", ""),
	}
	if c.InternalKey == "" {
		return Config{}, errors.New("ANALYST_API_KEY wajib diisi")
	}
	if c.RabbitURL == "" {
		return Config{}, errors.New("RABBITMQ_URL wajib diisi")
	}
	if p, err := strconv.Atoi(c.Port); err != nil || p < 1 || p > 65535 {
		return Config{}, errors.New("SITUATION_ANALYST_PORT harus antara 1 dan 65535")
	}

	var err error
	if c.Prefetch, err = getInt("ANALYST_PREFETCH", 50); err != nil {
		return Config{}, err
	}
	if c.QueueMaxLength, err = getInt("ANALYST_QUEUE_MAX_LENGTH", 2000); err != nil {
		return Config{}, err
	}
	if c.WindowMinutes, err = getInt("ANALYST_WINDOW_MINUTES", 15); err != nil {
		return Config{}, err
	}
	secs, err := getInt("ANALYST_SUMMARY_INTERVAL_SECONDS", 180)
	if err != nil {
		return Config{}, err
	}
	c.SummaryInterval = time.Duration(secs) * time.Second
	tsecs, err := getInt("LLM_TIMEOUT_SECONDS", 20)
	if err != nil {
		return Config{}, err
	}
	c.LLMTimeout = time.Duration(tsecs) * time.Second
	if c.LLMMaxPerHour, err = getInt("LLM_MAX_CALLS_PER_HOUR", 20); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) LLMEnabled() bool {
	return c.LLMBaseURL != "" && c.LLMModel != "" && c.LLMAPIKey != ""
}

func (c Config) LLMHost() string {
	u, err := url.Parse(c.LLMBaseURL)
	if err != nil {
		return ""
	}
	return u.Host
}

func (c Config) String() string {
	return fmt.Sprintf("port=%s queue=%s window=%dm llm_enabled=%v llm_model=%s llm_host=%s",
		c.Port, c.Queue, c.WindowMinutes, c.LLMEnabled(), c.LLMModel, c.LLMHost())
}
