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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"notifier/internal/hazard"
)

type config struct {
	rabbitURL    string
	exchange     string
	queue        string
	bindingKeys  []string
	minSeverity  hazard.Severity
	processDelay time.Duration
	prefetch     int
	healthPort   string
}

func loadConfig() (config, error) {
	rabbitURL := getEnv("RABBITMQ_URL", "")
	if rabbitURL == "" {
		return config{}, errors.New("RABBITMQ_URL is required")
	}
	keys := strings.Split(getEnv("NOTIFIER_BINDING_KEYS", "hazard.#"), ",")
	for i := range keys {
		keys[i] = strings.TrimSpace(keys[i])
	}
	return config{
		rabbitURL:    rabbitURL,
		exchange:     getEnv("RABBITMQ_EXCHANGE", "hazard_events"),
		queue:        getEnv("NOTIFIER_QUEUE", "notifier-queue"),
		bindingKeys:  keys,
		minSeverity:  hazard.Severity(strings.ToUpper(getEnv("NOTIFIER_MIN_SEVERITY", "WASPADA"))),
		processDelay: time.Duration(getInt("NOTIFIER_PROCESS_DELAY_MS", 0)) * time.Millisecond,
		prefetch:     getInt("NOTIFIER_PREFETCH", 10),
		healthPort:   getEnv("NOTIFIER_HEALTH_PORT", "8093"),
	}, nil
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "notifier")
	cfg, err := loadConfig()
	if err != nil {
		log.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var connected atomic.Bool
	go serveHealth(ctx, cfg.healthPort, &connected, log)

	seen := newSeenSet(10000)
	for ctx.Err() == nil {
		err := run(ctx, cfg, seen, &connected, log)
		connected.Store(false)
		if ctx.Err() != nil {
			break
		}
		log.Warn("consumer stopped, reconnecting", "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
		}
	}
	log.Info("notifier stopped")
}

func run(ctx context.Context, cfg config, seen *seenSet, connected *atomic.Bool, log *slog.Logger) error {
	dialStart := time.Now()
	conn, err := amqp.Dial(cfg.rabbitURL)
	if err != nil {
		log.Warn("rabbitmq call failed", "target", "rabbitmq", "operation", "connect",
			"duration_ms", time.Since(dialStart).Milliseconds(), "success", false, "err", err)
		return fmt.Errorf("dial: %w", err)
	}
	log.Info("rabbitmq call", "target", "rabbitmq", "operation", "connect",
		"duration_ms", time.Since(dialStart).Milliseconds(), "success", true)
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %w", err)
	}
	defer ch.Close()

	if err := ch.Qos(cfg.prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	q, err := ch.QueueDeclare(cfg.queue, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("queue declare: %w", err)
	}
	for _, key := range cfg.bindingKeys {
		if err := ch.QueueBind(q.Name, key, cfg.exchange, false, nil); err != nil {
			return fmt.Errorf("queue bind %q: %w", key, err)
		}
	}

	msgs, err := ch.Consume(q.Name, "", false /* manual ack */, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	connected.Store(true)
	log.Info("notifier listening", "exchange", cfg.exchange, "queue", q.Name, "binding_keys", cfg.bindingKeys,
		"min_alert_severity", cfg.minSeverity, "backlog_on_start", q.Messages)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return errors.New("delivery channel closed")
			}
			handle(ctx, cfg, seen, msg, log)
		}
	}
}

func handle(ctx context.Context, cfg config, seen *seenSet, msg amqp.Delivery, log *slog.Logger) {
	var ev hazard.Event
	if err := json.Unmarshal(msg.Body, &ev); err != nil {
		log.Warn("dropping unparsable message", "err", err, "routing_key", msg.RoutingKey, "correlation_id", msg.CorrelationId)
		_ = msg.Nack(false, false)
		return
	}

	key := ev.HazardID + "|" + string(ev.Severity)
	if seen.has(key) {
		log.Info("duplicate skipped", "hazard_id", ev.HazardID, "severity", ev.Severity, "correlation_id", msg.CorrelationId)
		_ = msg.Ack(false)
		return
	}

	if cfg.processDelay > 0 {
		select {
		case <-time.After(cfg.processDelay):
		case <-ctx.Done():
			return
		}
	}

	attrs := []any{
		"hazard_id", ev.HazardID, "hazard_type", ev.HazardType, "severity", ev.Severity, "area", ev.AreaName,
		"routing_key", msg.RoutingKey, "redelivered", msg.Redelivered,
		"age_ms", time.Since(ev.IngestedAt).Milliseconds(),
		"correlation_id", msg.CorrelationId,
	}
	if ev.Severity.Rank() >= cfg.minSeverity.Rank() {
		log.Info("ALERT dispatched (simulated)", attrs...)
	} else {
		log.Info("event received, below alert threshold", attrs...)
	}

	seen.add(key)
	_ = msg.Ack(false)
}

type seenSet struct {
	mu    sync.Mutex
	max   int
	order []string
	set   map[string]struct{}
}

func newSeenSet(max int) *seenSet { return &seenSet{max: max, set: make(map[string]struct{}, max)} }

func (s *seenSet) has(k string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.set[k]
	return ok
}

func (s *seenSet) add(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.set[k]; ok {
		return
	}
	if len(s.order) >= s.max {
		delete(s.set, s.order[0])
		s.order = s.order[1:]
	}
	s.order = append(s.order, k)
	s.set[k] = struct{}{}
}

func serveHealth(ctx context.Context, port string, connected *atomic.Bool, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !connected.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"disconnected"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sc, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sc)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("health server failed", "err", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return fallback
}
