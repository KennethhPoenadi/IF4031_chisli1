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

	"satellite-ingestor/internal/aggclient"
	"satellite-ingestor/internal/config"
	"satellite-ingestor/internal/feed"
	"satellite-ingestor/internal/fire"
	"satellite-ingestor/internal/pipeline"
	"satellite-ingestor/internal/province"
	"telemetry"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "satellite-ingestor")
	cfg, err := config.Load()
	if err != nil {
		log.Error("konfigurasi tidak valid", "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tel, err := telemetry.Init(ctx, "satellite-ingestor", log)
	if err != nil {
		log.Warn("telemetry disabled", "err", err)
	} else {
		defer func() {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tel.Shutdown(c)
		}()
	}

	provinces, err := province.Load(cfg.ProvinceTable)
	if err != nil {
		log.Error("tabel provinsi tidak terbaca", "err", err)
		os.Exit(1)
	}
	src := feed.New(cfg.FeedURL, cfg.FeedToken, cfg.HeartbeatTimeout)
	store := aggclient.New(cfg.AggregatorURL, cfg.AggregatorKey)
	if tel != nil {
		store.WithTransport(tel.Transport(nil))
	}
	p := &pipeline.Pipeline{
		Feed:      src,
		Store:     store,
		Engine:    fire.New(cfg.Lateness, provinces),
		BatchMax:  cfg.BatchMax,
		BatchWait: cfg.BatchWait,
		Log:       log,
		AllowGap:  func() { src.AllowGap = true },
	}

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: p.HealthHandler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server health gagal", "err", err)
		}
	}()
	log.Info("satellite-ingestor berjalan", "feed", cfg.FeedURL, "lateness_minutes", cfg.Lateness.Minutes(),
		"batch_max", cfg.BatchMax, "batch_wait_ms", cfg.BatchWait.Milliseconds())

	if err := p.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("pipeline berhenti", "err", err)
	}
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(c)
}
