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

	"aggregator/internal/api"
	"aggregator/internal/bmkgclient"
	"aggregator/internal/broker"
	"aggregator/internal/config"
	"aggregator/internal/httpx"
	"aggregator/internal/poller"
	"aggregator/internal/pvmbgclient"
	"aggregator/internal/store"

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

	mongoStart := time.Now()
	mongoClient, err := store.Connect(cfg.MongoURI)
	if err != nil {
		log.Error("mongo connect failed", "err", err, "target", "mongodb", "operation", "connect",
			"duration_ms", time.Since(mongoStart).Milliseconds(), "success", false)
		os.Exit(1)
	}
	log.Info("connected to mongo", "target", "mongodb", "operation", "connect",
		"duration_ms", time.Since(mongoStart).Milliseconds(), "success", true)
	defer mongoClient.Disconnect(context.Background()) // ini bwt nutup klo nnti koneksi mati

	hazardStore, err := store.NewHazardStore(mongoClient, cfg.MongoDBName, cfg.MongoHazardCollection)
	if err != nil {
		log.Error("mongo index setup failed", "err", err)
		os.Exit(1)
	}

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
	p := poller.New(bmkg, pvmbg, hazardStore, publisher, cfg.PollInterval, cfg.PVMBGStaleAfter, log)
	go p.Run(ctx)

	handler := api.NewHandler(hazardStore, p, publisher, log)
	routes := handler.Routes(cfg.AggregatorAPIKey, httpx.Observe(log))

	srv := &http.Server{
		Addr:              ":" + cfg.AggregatorPort,
		Handler:           routes,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
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
}
