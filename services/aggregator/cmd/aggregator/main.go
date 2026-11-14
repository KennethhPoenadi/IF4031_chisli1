package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"aggregator/internal/analysis"
	"aggregator/internal/api"
	"aggregator/internal/audit"
	"aggregator/internal/bmkgclient"
	"aggregator/internal/broker"
	"aggregator/internal/config"
	"aggregator/internal/dashboard"
	"aggregator/internal/httpx"
	"aggregator/internal/pantaiclient"
	"aggregator/internal/poller"
	"aggregator/internal/pvmbgclient"
	"aggregator/internal/resilience"
	"aggregator/internal/shardstore"
	"aggregator/internal/store"
	"telemetry"

	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load(".env", "../../../.env")

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "aggregator")

	cfg, err := config.Load()
	if err != nil {
		log.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tel, err := telemetry.Init(ctx, "aggregator", log)
	if err != nil {
		log.Warn("telemetry disabled", "err", err)
	} else {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tel.Shutdown(shutdownCtx)
		}()
	}

	hazardStore, closeStore, err := buildStore(cfg, log)
	if err != nil {
		log.Error("storage setup failed", "err", err)
		os.Exit(1)
	}
	defer closeStore()
	hazardStore.RegisterMetrics()
	go hazardStore.Run(ctx)

	rabbitStart := time.Now()
	publisher, err := broker.NewPublisher(cfg.RabbitMQURL, cfg.RabbitMQExchange, cfg.RabbitMQExchangeType)
	if err != nil {
		log.Error("rabbitmq connect failed", "err", err, "target", "rabbitmq", "operation", "connect",
			"duration_ms", time.Since(rabbitStart).Milliseconds(), "success", false)
		os.Exit(1)
	}
	log.Info("connected to rabbitmq", "exchange", cfg.RabbitMQExchange, "target", "rabbitmq", "operation", "connect",
		"duration_ms", time.Since(rabbitStart).Milliseconds(), "success", true)
	defer publisher.Close()

	bmkg := bmkgclient.New(cfg.BMKGBaseURL, cfg.BMKGAPIKey, cfg.BMKGTimeout, log)
	pvmbg := pvmbgclient.New(cfg.PVMBGBaseURL, cfg.PVMBGToken, cfg.PVMBGTimeout, log)
	if tel != nil {
		bmkg.WithTransport(tel.Transport(nil))
		pvmbg.WithTransport(tel.Transport(nil))
	}
	p := poller.New(bmkg, pvmbg, hazardStore, publisher, cfg.PollInterval, cfg.PVMBGStaleAfter, log)
	if cfg.PantaiEnabled {
		pantaiHTTP := &http.Client{Timeout: cfg.PantaiTimeout}
		if tel != nil {
			pantaiHTTP.Transport = tel.Transport(nil)
		}
		pantai, err := pantaiclient.New(pantaiclient.Config{
			BaseURL: cfg.PantaiBaseURL, Token: cfg.PantaiToken, Timeout: cfg.PantaiTimeout,
			HTTPClient: pantaiHTTP,
		})
		if err != nil {
			log.Error("invalid Sensor Pantai configuration", "err", err)
			os.Exit(1)
		}
		breaker := resilience.New(resilience.Config{
			FailureThreshold: cfg.PantaiCircuitFailures,
			OpenDuration:     cfg.PantaiCircuitOpen,
		})
		if err := p.ConfigureCoastal(poller.CoastalConfig{
			Source: pantai, StateStore: hazardStore, Breaker: breaker,
			PollInterval: cfg.PantaiPollInterval, StaleAfter: cfg.PantaiStaleAfter,
			StationCacheTTL: cfg.PantaiStationCacheTTL,
		}); err != nil {
			log.Error("failed to configure Sensor Pantai ingestion", "err", err)
			os.Exit(1)
		}
		log.Info("Sensor Pantai ingestion enabled", "base_url", cfg.PantaiBaseURL,
			"poll_interval_ms", cfg.PantaiPollInterval.Milliseconds(),
			"timeout_ms", cfg.PantaiTimeout.Milliseconds())
	}
	go p.Run(ctx)

	var analysisRT http.RoundTripper
	if tel != nil {
		analysisRT = tel.Transport(nil)
	}
	analysisClient, err := analysis.New(cfg.SituationAnalystURL, cfg.AggregatorAPIKey, 0, analysisRT)
	if err != nil {
		log.Error("invalid SITUATION_ANALYST_URL", "err", err)
		os.Exit(1)
	}
	dash := dashboard.New(hazardStore, cfg.DashboardToken).WithLogger(log)
	if analysisClient != nil {
		dash.WithAnalysis(analysisClient)
	}
	handler := api.NewHandler(hazardStore, p, publisher, log).
		WithAudit(audit.New(hazardStore, nil, cfg.AuditToken).WithLogger(log)).
		WithDashboard(dash)
	for _, w := range cfg.WeakTokens() {
		log.Warn("konfigurasi token lemah", "detail", w)
	}
	if cfg.AuditToken == "" {
		log.Warn("AUDIT_TOKEN kosong: Audit API menolak semua permintaan")
	}
	if cfg.DashboardToken == "" {
		log.Warn("DASHBOARD_TOKEN kosong: dashboard analitik menolak semua permintaan data")
	}
	routes := handler.Routes(cfg.AggregatorAPIKey, httpx.Observe(log))
	if tel != nil {
		routes = tel.Middleware(routes)
	}

	srv := &http.Server{
		Addr:              ":" + cfg.AggregatorPort,
		Handler:           routes,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("aggregator listening", "port", cfg.AggregatorPort)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "err", err)
		os.Exit(1)
	}
	<-shutdownDone
}

func buildStore(cfg config.Config, log *slog.Logger) (*shardstore.Router, func(), error) {
	type target struct{ name, uri string }
	var targets []target
	if len(cfg.StoreUnitNames) == 0 {
		targets = []target{{name: "store-0", uri: cfg.MongoURI}}
	} else {
		for _, name := range cfg.StoreUnitNames {
			targets = append(targets, target{name: name, uri: fmt.Sprintf(cfg.StoreURITemplate, name)})
		}
	}

	var mongoUnits []*store.MongoUnit
	units := make([]shardstore.Unit, 0, len(targets))
	closeAll := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, u := range mongoUnits {
			u.Close(ctx)
		}
	}
	for _, t := range targets {
		u, err := store.NewMongoUnit(t.name, t.uri, cfg.MongoDBName, cfg.MongoHazardCollection)
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("unit %s: %w", t.name, err)
		}
		mongoUnits = append(mongoUnits, u)
		units = append(units, u)
	}

	var resolvers shardstore.FallbackNodes
	if static, err := shardstore.ParseStaticNodes(cfg.StoreNodeMap); err != nil {
		closeAll()
		return nil, nil, err
	} else if len(static) > 0 {
		resolvers = append(resolvers, static)
	}
	if kube, err := shardstore.NewKubeNodes(); err == nil {
		resolvers = append(resolvers, kube)
	} else {
		log.Info("resolver node Kubernetes tidak aktif, nama node memakai STORE_NODE_MAP", "reason", err.Error())
	}

	router, err := shardstore.New(units, shardstore.Config{
		ReplicationFactor: cfg.StoreReplicationFactor,
		VNodes:            cfg.StoreVNodes,
		UnitTimeout:       cfg.StoreUnitTimeout,
		PingInterval:      cfg.StorePingInterval,
		RepairInterval:    cfg.StoreRepairInterval,
		Nodes:             resolvers,
		Log:               log,
	})
	if err != nil {
		closeAll()
		return nil, nil, err
	}
	log.Info("storage configured", "units", router.UnitNames(), "replication_factor", router.ReplicationFactor())
	return router, closeAll, nil
}
