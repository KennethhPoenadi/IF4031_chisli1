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

	PVMBGFieldAliases    string
	PVMBGFieldCoerce     string
	DLQRetryInterval     time.Duration
	FieldReportsEnabled  bool
	FieldReportDelay     time.Duration
	FieldReportPrefetch  int
	DeadLetterConsumerOn bool
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

	dlqRetry, err := getEnvSeconds("DLQ_RETRY_INTERVAL_SECONDS", 5)
	if err != nil {
		return Config{}, err
	}
	fieldPrefetch, err := getEnvInt("FIELD_REPORT_PREFETCH", 20)
	if err != nil {
		return Config{}, err
	}
	fieldDelay := time.Duration(0)
	if v := os.Getenv("FIELD_REPORT_PROCESS_DELAY_MS"); v != "" && v != "0" {
		if fieldDelay, err = getEnvMS("FIELD_REPORT_PROCESS_DELAY_MS", 0); err != nil {
			return Config{}, err
		}
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

		PVMBGFieldAliases:    getEnv("PVMBG_FIELD_ALIASES", ""),
		PVMBGFieldCoerce:     getEnv("PVMBG_FIELD_COERCE", ""),
		DLQRetryInterval:     dlqRetry,
		FieldReportsEnabled:  getEnv("FIELD_REPORTS_ENABLED", "true") != "false",
		FieldReportDelay:     fieldDelay,
		FieldReportPrefetch:  fieldPrefetch,
		DeadLetterConsumerOn: getEnv("DEAD_LETTER_CONSUMER_ENABLED", "true") != "false",
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
