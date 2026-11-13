package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port             string
	FeedURL          string
	FeedToken        string
	AggregatorURL    string
	AggregatorKey    string
	Lateness         time.Duration
	HeartbeatTimeout time.Duration
	BatchMax         int
	BatchWait        time.Duration
	ProvinceTable    string
}

func Load() (Config, error) {
	c := Config{
		Port:          get("SATELLITE_INGESTOR_PORT", "8095"),
		FeedURL:       get("SATELLITE_BASE_URL", "https://satellite.naufarrel.dev"),
		FeedToken:     os.Getenv("SATELLITE_TOKEN"),
		AggregatorURL: get("AGGREGATOR_URL", "http://aggregator:8090"),
		AggregatorKey: os.Getenv("AGGREGATOR_API_KEY"),
		ProvinceTable: os.Getenv("PROVINCE_TABLE_PATH"),
	}
	if c.FeedToken == "" {
		return Config{}, fmt.Errorf("SATELLITE_TOKEN wajib diisi")
	}
	if c.AggregatorKey == "" {
		return Config{}, fmt.Errorf("AGGREGATOR_API_KEY wajib diisi")
	}
	var err error
	if c.Lateness, err = minutes("SATELLITE_LATENESS_MINUTES", 360); err != nil {
		return Config{}, err
	}
	if c.HeartbeatTimeout, err = seconds("SATELLITE_HEARTBEAT_TIMEOUT_SECONDS", 30); err != nil {
		return Config{}, err
	}
	if c.BatchMax, err = integer("SATELLITE_BATCH_MAX", 500); err != nil {
		return Config{}, err
	}
	ms, err := integer("SATELLITE_BATCH_WAIT_MS", 250)
	if err != nil {
		return Config{}, err
	}
	c.BatchWait = time.Duration(ms) * time.Millisecond
	return c, nil
}

func get(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func integer(k string, def int) (int, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s harus bilangan bulat positif", k)
	}
	return n, nil
}

func minutes(k string, def int) (time.Duration, error) {
	n, err := integer(k, def)
	return time.Duration(n) * time.Minute, err
}

func seconds(k string, def int) (time.Duration, error) {
	n, err := integer(k, def)
	return time.Duration(n) * time.Second, err
}
