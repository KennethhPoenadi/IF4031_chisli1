package config

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Client struct {
	ID     string
	Secret string
	Scope  string // space separated, e.g. "read:summary read:raw"
}

type Config struct {
	Port        string
	Issuer      string
	SigningKey  ed25519.PrivateKey
	AccessTTL   time.Duration
	RefreshTTL  time.Duration
	InternalKey string 
	Clients     []Client
}

func Load() (Config, error) {
	seedB64, err := required("AUTH_SIGNING_SEED")
	if err != nil {
		return Config{}, err
	}
	seed, err := base64.StdEncoding.DecodeString(seedB64)
	if err != nil || len(seed) != ed25519.SeedSize {
		return Config{}, fmt.Errorf("AUTH_SIGNING_SEED must be base64 of exactly %d bytes (openssl rand -base64 32)", ed25519.SeedSize)
	}
	internalKey, err := required("AUTH_INTERNAL_KEY")
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		Port:        getEnv("AUTH_PORT", "8091"),
		Issuer:      getEnv("AUTH_TOKEN_ISSUER", "bnpb-auth"),
		SigningKey:  ed25519.NewKeyFromSeed(seed),
		AccessTTL:   time.Duration(getInt("AUTH_ACCESS_TTL_SECONDS", 60)) * time.Second,
		RefreshTTL:  time.Duration(getInt("AUTH_REFRESH_TTL_SECONDS", 600)) * time.Second,
		InternalKey: internalKey,
	}

	defs := []struct{ prefix, scope string }{
		{"AUTH_CLIENT_MEDIA", "read:summary"},
		{"AUTH_CLIENT_FIELD", "read:summary read:raw"},
		{"AUTH_CLIENT_OPS", "read:summary read:raw"},
	}
	ids, secrets := map[string]bool{}, map[string]bool{}
	for _, d := range defs {
		id, err := required(d.prefix + "_ID")
		if err != nil {
			return Config{}, err
		}
		secret, err := required(d.prefix + "_SECRET")
		if err != nil {
			return Config{}, err
		}
		if ids[id] || secrets[secret] {
			return Config{}, fmt.Errorf("%s: client IDs and secrets must be unique per client", d.prefix)
		}
		ids[id], secrets[secret] = true, true
		cfg.Clients = append(cfg.Clients, Client{ID: id, Secret: secret, Scope: d.scope})
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
