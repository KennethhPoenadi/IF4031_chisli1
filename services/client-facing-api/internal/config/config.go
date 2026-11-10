package config

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port              string
	Issuer            string
	AggregatorURL     string
	AggregatorKey     string
	AggregatorTimeout time.Duration
	AuthURL           string
	AuthInternalKey   string
	PinnedPublicKey   ed25519.PublicKey
	RevocationPoll    time.Duration
	MaxInflight       int
	PriorityReserved  int
	MaxPriorityQueued int
	PriorityClientID  string
}

func Load() (Config, error) {
	aggKey, err := required("AGGREGATOR_API_KEY")
	if err != nil {
		return Config{}, err
	}
	authKey, err := required("AUTH_INTERNAL_KEY")
	if err != nil {
		return Config{}, err
	}
	maxInflight := getInt("CLIENT_API_MAX_INFLIGHT", 200)
	if maxInflight < 2 {
		return Config{}, fmt.Errorf("CLIENT_API_MAX_INFLIGHT must be at least 2 when priority admission is enabled")
	}
	defaultReserved := maxInflight / 10
	if defaultReserved < 1 {
		defaultReserved = 1
	}
	priorityReserved := getInt("CLIENT_API_PRIORITY_RESERVED", defaultReserved)
	if priorityReserved >= maxInflight {
		return Config{}, fmt.Errorf("CLIENT_API_PRIORITY_RESERVED must be less than CLIENT_API_MAX_INFLIGHT")
	}
	priorityClientID := strings.TrimSpace(getEnv("CLIENT_API_PRIORITY_CLIENT_ID", getEnv("AUTH_CLIENT_FIELD_ID", "field-demo")))
	if priorityClientID == "" {
		return Config{}, fmt.Errorf("CLIENT_API_PRIORITY_CLIENT_ID must not be blank")
	}

	cfg := Config{
		Port:              getEnv("CLIENT_API_PORT", "8080"),
		Issuer:            getEnv("AUTH_TOKEN_ISSUER", "bnpb-auth"),
		AggregatorURL:     getEnv("AGGREGATOR_URL", "http://aggregator:8090"),
		AggregatorKey:     aggKey,
		AggregatorTimeout: time.Duration(getInt("CLIENT_API_AGGREGATOR_TIMEOUT_MS", 1500)) * time.Millisecond,
		AuthURL:           getEnv("AUTH_SERVICE_URL", "http://auth-service:8091"),
		AuthInternalKey:   authKey,
		RevocationPoll:    time.Duration(getInt("CLIENT_API_REVOCATION_POLL_MS", 250)) * time.Millisecond,
		MaxInflight:       maxInflight,
		PriorityReserved:  priorityReserved,
		MaxPriorityQueued: getInt("CLIENT_API_MAX_PRIORITY_QUEUED", 400),
		PriorityClientID:  priorityClientID,
	}
	if pk := os.Getenv("AUTH_PUBLIC_KEY"); pk != "" {
		raw, err := base64.StdEncoding.DecodeString(pk)
		if err != nil || len(raw) != ed25519.PublicKeySize {
			return Config{}, fmt.Errorf("AUTH_PUBLIC_KEY must be base64 of a %d-byte Ed25519 public key", ed25519.PublicKeySize)
		}
		cfg.PinnedPublicKey = ed25519.PublicKey(raw)
	}
	return cfg, nil
}

func required(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("%s must be set", key)
	}
	return v, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
