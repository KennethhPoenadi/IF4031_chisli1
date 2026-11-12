package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	amqp "github.com/rabbitmq/amqp091-go"

	"dashboard-updater/internal/config"
	"dashboard-updater/internal/hazard"
	"telemetry"
)

// DLXExchange sama dengan yang dideklarasikan Aggregator (internal/amqpx).
const DLXExchange = "hazard_events.dlx"

func Run(ctx context.Context, cfg config.Config, seen *SeenSet, state *State, connected *atomic.Bool, log *slog.Logger) error {
	dialStart := time.Now()
	conn, err := amqp.Dial(cfg.RabbitURL)
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

	if err := ch.Qos(cfg.Prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	// Pesan yang ditolak dipindah ke dead-letter exchange, lalu disimpan Aggregator di DLQ (tidak dibuang)
	if err := ch.ExchangeDeclare(DLXExchange, "fanout", true, false, false, false, nil); err != nil {
		return fmt.Errorf("dlx declare: %w", err)
	}
	q, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, amqp.Table{"x-dead-letter-exchange": DLXExchange})
	if err != nil {
		return fmt.Errorf("queue declare: %w", err)
	}

	if err := ch.QueueBind(q.Name, cfg.BindingKey, cfg.Exchange, false, nil); err != nil {
		return fmt.Errorf("queue bind %q: %w", cfg.BindingKey, err)
	}

	msgs, err := ch.Consume(q.Name, "", false /* manual ack */, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	connected.Store(true)
	log.Info("dashboard-updater listening", "action", "listen", "exchange", cfg.Exchange, "queue", q.Name, "binding_key", cfg.BindingKey)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return errors.New("delivery channel closed")
			}
			handle(seen, state, msg, log)
		}
	}
}

var (
	messages = telemetry.Counter("dashboard_updater_messages", "pesan yang diproses dashboard-updater, per hasil")
	latency  = telemetry.Histogram("dashboard_update_latency", "detik dari event disimpan aggregator sampai dashboard diperbarui")
)

func handle(seen *SeenSet, state *State, msg amqp.Delivery, log *slog.Logger) {
	ctx, endSpan := telemetry.StartConsumerSpan(context.Background(), "process hazard event", msg.Headers,
		"messaging.system", "rabbitmq", "messaging.rabbitmq.destination.routing_key", msg.RoutingKey,
		"correlation.id", msg.CorrelationId)
	outcome := "applied"
	defer func() {
		endSpan(nil)
		messages(ctx, 1, "outcome", outcome)
	}()

	var event hazard.Event
	if !utf8.Valid(msg.Body) {
		outcome = "dead_lettered"
		log.Warn("pesan bukan UTF-8 valid dipindah ke DLQ", "action", "dead_letter", "routing_key", msg.RoutingKey, "correlation_id", msg.CorrelationId)
		_ = msg.Nack(false, false)
		return
	}
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		outcome = "dead_lettered"
		log.Warn("pesan tidak terbaca dipindah ke DLQ", "action", "dead_letter", "err", err, "routing_key", msg.RoutingKey, "correlation_id", msg.CorrelationId)
		_ = msg.Nack(false, false)
		return
	}

	key := event.HazardID + "|" + string(event.Severity)
	if seen.Has(key) {
		outcome = "duplicate"
		log.Info("duplicate skipped", "action", "duplicate", "hazard_id", event.HazardID, "severity", event.Severity, "correlation_id", msg.CorrelationId)
		_ = msg.Ack(false)
		return
	}

	log.Info("received hazard event", "action", "received",
		"hazard_id", event.HazardID, "hazard_type", event.HazardType, "severity", event.Severity,
		"routing_key", msg.RoutingKey, "redelivered", msg.Redelivered, "correlation_id", msg.CorrelationId)

	state.Apply(event)
	seen.Add(key)
	latency(ctx, time.Since(event.IngestedAt).Seconds(), "severity", string(event.Severity))
	_ = msg.Ack(false)
}

type SeenSet struct {
	mu    sync.Mutex
	max   int
	order []string
	set   map[string]struct{}
}

func NewSeenSet(max int) *SeenSet {
	return &SeenSet{max: max, set: make(map[string]struct{}, max)}
}

func (s *SeenSet) Has(k string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.set[k]
	return ok
}

func (s *SeenSet) Add(k string) {
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
