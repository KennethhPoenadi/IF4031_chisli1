package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"dashboard-updater/internal/config"
	"dashboard-updater/internal/consumer"
	"dashboard-updater/internal/health"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "dashboard-updater")

	cfg, err := config.Load()
	if err != nil {
		log.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var connected atomic.Bool
	state := consumer.NewState()
	go health.Serve(ctx, cfg.HealthPort, &connected, state, log)

	seen := consumer.NewSeenSet(10000)
	for ctx.Err() == nil {
		err := consumer.Run(ctx, cfg, seen, state, &connected, log)
		connected.Store(false)
		if ctx.Err() != nil {
			break
		}
		log.Warn("consumer stopped, reconnecting", "action", "reconnect", "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
		}
	}
	log.Info("dashboard-updater stopped", "action", "stopped")
}
