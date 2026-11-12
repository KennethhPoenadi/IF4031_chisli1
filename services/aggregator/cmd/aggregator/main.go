package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"unicode/utf8"

	"aggregator/internal/amqpx"
	"aggregator/internal/analysis"
	"aggregator/internal/api"
	"aggregator/internal/audit"
	"aggregator/internal/bmkgclient"
	"aggregator/internal/broker"
	"aggregator/internal/config"
	"aggregator/internal/dashboard"
	"aggregator/internal/deadletter"
	"aggregator/internal/dlq"
	"aggregator/internal/fieldreport"
	"aggregator/internal/hazard"
	"aggregator/internal/httpx"
	"aggregator/internal/ingestapi"
	"aggregator/internal/mapper"
	"aggregator/internal/poller"
	"aggregator/internal/pvmbgclient"
	"aggregator/internal/satingest"
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

	hazardStore, docStore, closeStore, err := buildStore(cfg, log)
	if err != nil {
		log.Error("storage setup failed", "err", err)
		os.Exit(1)
	}
	defer closeStore()
	hazardStore.RegisterMetrics()
	hazardStore.SetTombstones(docStore)
	go hazardStore.Run(ctx)
	go docStore.Run(ctx)

	schemaRules, err := mapper.ParseRules(cfg.PVMBGFieldAliases, cfg.PVMBGFieldCoerce)
	if err != nil {
		log.Error("invalid PVMBG_FIELD_ALIASES / PVMBG_FIELD_COERCE", "err", err)
		os.Exit(1)
	}
	deadLetters := dlq.New(docStore)
	deadLetters.RegisterMetrics(ctx)

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
	p.WithDeadLetters(deadLetters, schemaRules)
	if !schemaRules.Empty() {
		log.Info("aturan penyesuaian skema PVMBG aktif", "aliases", cfg.PVMBGFieldAliases, "coerce", cfg.PVMBGFieldCoerce)
	}
	go p.Run(ctx)

	var reports *fieldreport.Processor
	if cfg.FieldReportsEnabled {
		reports = fieldreport.New(docStore, log, cfg.FieldReportDelay)
		go reports.Run(ctx, cfg.RabbitMQURL, cfg.FieldReportPrefetch)
	}
	if cfg.DeadLetterConsumerOn {
		go deadletter.New(deadLetters, log).Run(ctx, cfg.RabbitMQURL)
	}
	processors := func() map[string]dlq.Processor {
		procs := p.Processors()
		procs[dlq.KindAMQP] = amqpProcessor(reports, publisher)
		return procs
	}
	go deadLetters.RunRetry(ctx, cfg.DLQRetryInterval, processors(), nil, log)
	ingest := &ingestapi.Handler{DLQ: deadLetters, Processors: processors, Reports: reports}
	satellite := satingest.New(docStore, hazardStore, publisher, log)
	internalAuth := httpx.RequireInternalKey(cfg.AggregatorAPIKey)
	statsAuth := internalOrAudit(cfg.AggregatorAPIKey, cfg.AuditToken)

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
		WithAudit(audit.New(hazardStore, deadLetters, cfg.AuditToken).WithLogger(log)).
		WithDashboard(dash).
		WithRoutes(func(mux *http.ServeMux) {
			ingest.Register(mux, internalAuth)
			satellite.Register(mux, internalAuth, statsAuth)
		})
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

// menerima internal key Aggregator atau token Audit (untuk statistik yang dibaca penguji).
func internalOrAudit(internalKey, auditToken string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if k := r.Header.Get("X-Internal-Key"); k != "" && httpx.TokenEqual(k, internalKey) {
				next.ServeHTTP(w, r)
				return
			}
			if auditToken != "" && httpx.TokenEqual(r.Header.Get("Authorization"), "Bearer "+auditToken) {
				next.ServeHTTP(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "unauthorized", "message": "token tidak valid"}})
		})
	}
}

// memproses ulang pesan broker yang sempat ditolak konsumer.
func amqpProcessor(reports *fieldreport.Processor, pub *broker.Publisher) dlq.Processor {
	return func(ctx context.Context, e dlq.Entry) error {
		if e.Origin == amqpx.FieldReportsQueue {
			if reports == nil {
				return errors.New("jalur laporan tidak aktif")
			}
			return reports.DLQProcessor(ctx, e)
		}
		raw, err := e.RawPayload()
		if err != nil || !utf8.Valid(raw) {
			return dlq.Permanent("malformed: payload bukan UTF-8 valid")
		}
		var ev hazard.Event
		if err := json.Unmarshal(raw, &ev); err != nil {
			return dlq.Permanent("malformed: " + err.Error())
		}
		if ev.HazardID == "" || ev.Source == "" || ev.HazardType == "" {
			return dlq.Permanent("schema_incompatible: HazardEvent tanpa hazard_id/source/hazard_type")
		}
		return pub.Publish(ctx, ev, e.CorrelationID)
	}
}

func buildStore(cfg config.Config, log *slog.Logger) (*shardstore.Router, *shardstore.DocStore, func(), error) {
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
			return nil, nil, nil, fmt.Errorf("unit %s: %w", t.name, err)
		}
		mongoUnits = append(mongoUnits, u)
		units = append(units, u)
	}

	var resolvers shardstore.FallbackNodes
	if static, err := shardstore.ParseStaticNodes(cfg.StoreNodeMap); err != nil {
		closeAll()
		return nil, nil, nil, err
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
		return nil, nil, nil, err
	}
	docUnits := make([]shardstore.DocUnit, 0, len(mongoUnits))
	for _, u := range mongoUnits {
		docUnits = append(docUnits, u)
	}
	docs, err := shardstore.NewDocStore(router, docUnits)
	if err != nil {
		closeAll()
		return nil, nil, nil, err
	}
	log.Info("storage configured", "units", router.UnitNames(), "replication_factor", router.ReplicationFactor())
	return router, docs, closeAll, nil
}
