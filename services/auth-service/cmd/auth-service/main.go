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

	"auth-service/internal/auth"
	"auth-service/internal/config"
	"telemetry"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "auth-service")

	cfg, err := config.Load()
	if err != nil {
		log.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	svc := auth.NewService(cfg)
	handler := auth.NewHandler(svc, cfg.InternalKey, log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tel, err := telemetry.Init(ctx, "auth-service", log)
	if err != nil {
		log.Warn("telemetry disabled", "err", err)
	} else {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tel.Shutdown(shutdownCtx)
		}()
	}

	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				svc.Sweep()
			case <-ctx.Done():
				return
			}
		}
	}()

	routes := handler.Routes()
	if tel != nil {
		routes = tel.Middleware(routes)
	}
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           routes,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
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

	log.Info("auth-service listening", "port", cfg.Port, "access_ttl_s", int(cfg.AccessTTL.Seconds()), "refresh_ttl_s", int(cfg.RefreshTTL.Seconds()))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "err", err)
		os.Exit(1)
	}
	<-shutdownDone
}
