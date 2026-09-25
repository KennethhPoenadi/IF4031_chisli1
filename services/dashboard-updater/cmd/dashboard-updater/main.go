package main

import (
	"context"
	"fmt"
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
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var connected atomic.Bool
	go health.Serve(ctx, cfg.HealthPort, &connected)

	seen := consumer.NewSeenSet(10000)
	for ctx.Err() == nil {
		err := consumer.Run(ctx, cfg, seen, &connected)
		connected.Store(false)
		if ctx.Err() != nil {
			break
		}
		fmt.Println("consumer stopped, reconnecting:", err)
		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
		}
	}
	fmt.Println("dashboard-updater stopped")
}
