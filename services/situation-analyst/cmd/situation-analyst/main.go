package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"situation-analyst/internal/anomaly"
	"situation-analyst/internal/api"
	"situation-analyst/internal/config"
	"situation-analyst/internal/consumer"
	"situation-analyst/internal/engine"
	"situation-analyst/internal/llm"
	"telemetry"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "situation-analyst")

	cfg, err := config.Load()
	if err != nil {
		log.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tel, err := telemetry.Init(ctx, "situation-analyst", log)
	if err != nil {
		log.Warn("telemetry disabled", "err", err)
	} else {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tel.Shutdown(shutdownCtx)
		}()
	}

	llmCfg := llm.Config{BaseURL: cfg.LLMBaseURL, Model: cfg.LLMModel, APIKey: cfg.LLMAPIKey,
		Timeout: cfg.LLMTimeout, MaxPerHour: cfg.LLMMaxPerHour}
	if tel != nil {
		llmCfg.Transport = tel.Transport(nil)
	}
	client, err := llm.New(llmCfg, nil, log)
	if err != nil {
		log.Error("invalid LLM configuration", "err", err)
		os.Exit(1)
	}
	log.Info("situation-analyst starting", "config", cfg.String())
	if !client.Enabled() {
		log.Warn("LLM tidak dikonfigurasi: ringkasan memakai templat, deteksi anomali tetap berjalan")
	}

	eng := engine.New(engine.Config{WindowMinutes: cfg.WindowMinutes, SummaryInterval: cfg.SummaryInterval,
		LLMDeadline: cfg.LLMTimeout + 5*time.Second, Anomaly: anomaly.DefaultConfig()}, client, log, nil)

	var connected atomic.Bool
	telemetry.GaugeFunc("analyst_active_anomalies", "jumlah anomali yang sedang aktif", func() []telemetry.Sample {
		return []telemetry.Sample{{Value: float64(eng.ActiveAnomalies())}}
	})
	telemetry.GaugeFunc("llm_breaker_open", "1 bila circuit breaker LLM terbuka", func() []telemetry.Sample {
		v := 0.0
		if client.BreakerOpen() {
			v = 1
		}
		return []telemetry.Sample{{Value: v}}
	})
	telemetry.GaugeFunc("analyst_rabbitmq_connected", "1 bila terhubung ke RabbitMQ", func() []telemetry.Sample {
		v := 0.0
		if connected.Load() {
			v = 1
		}
		return []telemetry.Sample{{Value: v}}
	})

	go eng.Run(ctx)

	ccfg := consumer.Config{URL: cfg.RabbitURL, Exchange: cfg.Exchange, Queue: cfg.Queue, Binding: cfg.BindingKey,
		Prefetch: cfg.Prefetch, MaxLength: cfg.QueueMaxLength}
	go func() {
		for ctx.Err() == nil {
			err := consumer.Run(ctx, ccfg, eng, &connected, log)
			connected.Store(false)
			if ctx.Err() != nil {
				return
			}
			log.Warn("consumer stopped, reconnecting", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(3 * time.Second):
			}
		}
	}()

	routes := api.Routes(eng, cfg.InternalKey, &connected)
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

	log.Info("situation-analyst listening", "port", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "err", err)
		os.Exit(1)
	}
	<-shutdownDone
}
