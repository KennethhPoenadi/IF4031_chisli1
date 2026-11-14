package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"client-facing-api/internal/api"
	"client-facing-api/internal/authn"
	"client-facing-api/internal/broker"
	"client-facing-api/internal/config"
	"client-facing-api/internal/httpx"
	"client-facing-api/internal/priority"
	"client-facing-api/internal/upstream"
	"telemetry"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "client-facing-api")

	cfg, err := config.Load()
	if err != nil {
		log.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tel, err := telemetry.Init(ctx, "client-facing-api", log)
	if err != nil {
		log.Warn("telemetry disabled", "err", err)
	} else {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tel.Shutdown(shutdownCtx)
		}()
	}

	pub := cfg.PinnedPublicKey
	if pub == nil {
		pub, err = authn.FetchPublicKey(ctx, cfg.AuthURL, 30, log)
		if err != nil {
			log.Error("could not obtain token verification key", "err", err)
			os.Exit(1)
		}
	}
	verifier := authn.NewVerifier(pub, cfg.Issuer)
	go verifier.SyncRevocations(ctx, cfg.AuthURL, cfg.AuthInternalKey, cfg.RevocationPoll, log)
	admission, err := priority.New(priority.Config{
		MaxInFlight:         cfg.MaxInflight,
		ReservedForPriority: cfg.PriorityReserved,
		MaxPriorityQueued:   cfg.MaxPriorityQueued,
		IsPriority: func(r *http.Request) bool {
			claims := authn.ClaimsFrom(r.Context())
			return claims != nil && claims.Subject == cfg.PriorityClientID
		},
	})
	if err != nil {
		log.Error("invalid priority admission configuration", "err", err)
		os.Exit(1)
	}

	agg := upstream.New(cfg.AggregatorURL, cfg.AggregatorKey, cfg.AggregatorTimeout, cfg.MaxInflight, log)
	opsAgg := upstream.New(cfg.AggregatorURL, cfg.AggregatorKey, cfg.OpsTimeout, 4, log)
	apiHandler := api.NewHandler(agg).WithOps(opsAgg)
	if cfg.RabbitMQURL != "" {
		reports := broker.New(cfg.RabbitMQURL)
		defer reports.Close()
		apiHandler.WithFieldReports(reports, cfg.FieldReportPublishTTL)
	} else {
		log.Warn("RABBITMQ_URL kosong: POST /field-reports menjawab 503")
	}
	handler := apiHandler.Routes(verifier, admission.Middleware, httpx.Observe(log))
	if tel != nil {
		// span dan metrik dihitung paling luar, jadi penolakan 429 oleh admission juga terukur
		agg.WithTransport(tel.Transport)
		opsAgg.WithTransport(tel.Transport)
		handler = tel.Middleware(handler)
	}

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler,
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

	log.Info("client-facing-api listening", "port", cfg.Port, "max_inflight", cfg.MaxInflight,
		"priority_reserved", cfg.PriorityReserved, "max_priority_queued", cfg.MaxPriorityQueued,
		"priority_client", cfg.PriorityClientID, "aggregator_timeout_ms", cfg.AggregatorTimeout.Milliseconds())
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "err", err)
		os.Exit(1)
	}
	<-shutdownDone
}
