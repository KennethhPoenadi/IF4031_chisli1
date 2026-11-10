package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chisli1/if4031-m1/services/pvmbg-mock/internal/pvmbg"
	"telemetry"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	config, err := pvmbg.ConfigFromEnv()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Telemetry gagal dimulai tidak boleh mematikan layanan: lanjut tanpa instrumentasi dan catat peringatan.
	tel, err := telemetry.Init(ctx, "pvmbg-mock", logger)
	if err != nil {
		logger.Warn("telemetry disabled", "err", err)
	} else {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tel.Shutdown(shutdownCtx)
		}()
	}
	store := pvmbg.NewStore(config.SeedCount, config.RandomSeed, time.Now)
	go store.RunGenerator(ctx, config.GenerationInterval)

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", config.Port),
		Handler:           instrument(tel, pvmbg.NewServer(config, store, os.Stdout).Handler()),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("service started", "service", "pvmbg-mock", "port", config.Port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server stopped unexpectedly", "error", err)
		os.Exit(1)
	}
	// ListenAndServe returns as soon as Shutdown is called; wait for in-flight requests to drain.
	<-shutdownDone
}

// instrument membungkus handler dengan trace dan metrik bila telemetry aktif.
func instrument(tel *telemetry.Provider, h http.Handler) http.Handler {
	if tel == nil {
		return h
	}
	return tel.Middleware(h)
}
