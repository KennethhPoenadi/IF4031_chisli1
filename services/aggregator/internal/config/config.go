package config

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
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

	AuditToken          string
	SituationAnalystURL string
	DashboardToken      string

	StoreUnitNames         []string
	StoreURITemplate       string
	StoreReplicationFactor int
	StoreVNodes            int
	StoreUnitTimeout       time.Duration
	StorePingInterval      time.Duration
	StoreRepairInterval    time.Duration
	StoreNodeMap           string

	BMKGBaseURL string
	BMKGAPIKey  string
	BMKGTimeout time.Duration

	PVMBGBaseURL string
	PVMBGToken   string
	PVMBGTimeout time.Duration

	PollInterval    time.Duration
	PVMBGStaleAfter time.Duration

	PantaiEnabled         bool
	PantaiBaseURL         string
	PantaiToken           string
	PantaiTimeout         time.Duration
	PantaiPollInterval    time.Duration
	PantaiStaleAfter      time.Duration
	PantaiStationCacheTTL time.Duration
	PantaiCircuitFailures int
	PantaiCircuitOpen     time.Duration
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
	pantaiEnabled, err := getEnvBool("AGGREGATOR_PANTAI_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	pantaiTimeout, err := getEnvMS("AGGREGATOR_PANTAI_TIMEOUT_MS", 1500)
	if err != nil {
		return Config{}, err
	}
	pantaiPollInterval, err := getEnvMS("AGGREGATOR_PANTAI_POLL_INTERVAL_MS", 1000)
	if err != nil {
		return Config{}, err
	}
	pantaiStaleAfter, err := getEnvSeconds("AGGREGATOR_PANTAI_STALE_AFTER_SECONDS", 30)
	if err != nil {
		return Config{}, err
	}
	pantaiStationCacheTTL, err := getEnvSeconds("AGGREGATOR_PANTAI_STATION_CACHE_TTL_SECONDS", 300)
	if err != nil {
		return Config{}, err
	}
	pantaiCircuitFailures, err := getEnvInt("AGGREGATOR_PANTAI_CIRCUIT_FAILURES", 3)
	if err != nil {
		return Config{}, err
	}
	pantaiCircuitOpen, err := getEnvSeconds("AGGREGATOR_PANTAI_CIRCUIT_OPEN_SECONDS", 10)
	if err != nil {
		return Config{}, err
	}
	pantaiToken := strings.TrimSpace(getEnv("PANTAI_AUTH_TOKEN", ""))
	if pantaiEnabled && pantaiToken == "" {
		return Config{}, fmt.Errorf("PANTAI_AUTH_TOKEN is required when AGGREGATOR_PANTAI_ENABLED is true")
	}

	storeRF, err := getEnvInt("STORE_REPLICATION_FACTOR", 2)
	if err != nil {
		return Config{}, err
	}
	storeVNodes, err := getEnvInt("STORE_VNODES", 128)
	if err != nil {
		return Config{}, err
	}
	storeTimeout, err := getEnvMS("STORE_UNIT_TIMEOUT_MS", 2000)
	if err != nil {
		return Config{}, err
	}
	storePing, err := getEnvSeconds("STORE_PING_INTERVAL_SECONDS", 2)
	if err != nil {
		return Config{}, err
	}
	storeRepair, err := getEnvSeconds("STORE_REPAIR_INTERVAL_SECONDS", 30)
	if err != nil {
		return Config{}, err
	}

	storeNames := splitList(getEnv("STORE_UNIT_NAMES", ""))
	storeTemplate := getEnv("STORE_URI_TEMPLATE", "")
	if len(storeNames) > 0 && !strings.Contains(storeTemplate, "%s") {
		return Config{}, fmt.Errorf("STORE_URI_TEMPLATE wajib memuat %%s (nama unit) bila STORE_UNIT_NAMES diisi")
	}

	return Config{
		AuditToken:             getEnv("AUDIT_TOKEN", ""),
		DashboardToken:         getEnv("DASHBOARD_TOKEN", ""),
		SituationAnalystURL:    getEnv("SITUATION_ANALYST_URL", ""),
		StoreUnitNames:         storeNames,
		StoreURITemplate:       storeTemplate,
		StoreReplicationFactor: storeRF,
		StoreVNodes:            storeVNodes,
		StoreUnitTimeout:       storeTimeout,
		StorePingInterval:      storePing,
		StoreRepairInterval:    storeRepair,
		StoreNodeMap:           getEnv("STORE_NODE_MAP", ""),
		MongoURI:               getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDBName:            getEnv("MONGO_DB_NAME", "bnpb_canonical"),
		MongoHazardCollection:  getEnv("MONGO_HAZARD_COLLECTION", "hazard_events"),
		RabbitMQURL:            rabbitMQURL,
		RabbitMQExchange:       getEnv("RABBITMQ_EXCHANGE", "hazard_events"),
		RabbitMQExchangeType:   getEnv("RABBITMQ_EXCHANGE_TYPE", "topic"),
		AggregatorPort:         strconv.Itoa(port),
		AggregatorAPIKey:       aggregatorAPIKey,

		BMKGBaseURL: getEnv("BMKG_BASE_URL", "http://localhost:8081"),
		BMKGAPIKey:  bmkgAPIKey,
		BMKGTimeout: bmkgTimeout,

		PVMBGBaseURL: getEnv("PVMBG_BASE_URL", "http://localhost:8082"),
		PVMBGToken:   pvmbgToken,
		PVMBGTimeout: pvmbgTimeout,

		PollInterval:    pollInterval,
		PVMBGStaleAfter: pvmbgStaleAfter,

		PantaiEnabled:         pantaiEnabled,
		PantaiBaseURL:         getEnv("PANTAI_BASE_URL", "http://sensor-pantai:8080"),
		PantaiToken:           pantaiToken,
		PantaiTimeout:         pantaiTimeout,
		PantaiPollInterval:    pantaiPollInterval,
		PantaiStaleAfter:      pantaiStaleAfter,
		PantaiStationCacheTTL: pantaiStationCacheTTL,
		PantaiCircuitFailures: pantaiCircuitFailures,
		PantaiCircuitOpen:     pantaiCircuitOpen,
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

func getEnvInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

func getEnvBool(key string, fallback bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return parsed, nil
}

func splitList(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

const MinTokenLength = 32

func (c Config) WeakTokens() []string {
	var out []string
	for name, v := range map[string]string{"AUDIT_TOKEN": c.AuditToken, "DASHBOARD_TOKEN": c.DashboardToken} {
		if v != "" && len(v) < MinTokenLength {
			out = append(out, name+" lebih pendek dari "+strconv.Itoa(MinTokenLength)+" karakter")
		}
	}
	if c.AuditToken != "" && c.AuditToken == c.DashboardToken {
		out = append(out, "AUDIT_TOKEN dan DASHBOARD_TOKEN sama: pisahkan agar pemegang dashboard tidak memegang token penguji")
	}
	sort.Strings(out)
	return out
}
