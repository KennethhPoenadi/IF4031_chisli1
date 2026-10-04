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
	"client-facing-api/internal/config"
	"client-facing-api/internal/httpx"
	"client-facing-api/internal/upstream"
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

	agg := upstream.New(cfg.AggregatorURL, cfg.AggregatorKey, cfg.AggregatorTimeout, cfg.MaxInflight, log)
	handler := api.NewHandler(agg).Routes(verifier, httpx.Limit(cfg.MaxInflight), httpx.Observe(log))

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler,
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

	log.Info("client-facing-api listening", "port", cfg.Port, "max_inflight", cfg.MaxInflight, "aggregator_timeout_ms", cfg.AggregatorTimeout.Milliseconds())
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "err", err)
		os.Exit(1)
	}
}
