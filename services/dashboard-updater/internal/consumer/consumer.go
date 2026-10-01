package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"

	"dashboard-updater/internal/config"
	"dashboard-updater/internal/hazard"
)

func Run(ctx context.Context, cfg config.Config, seen *SeenSet, state *State, connected *atomic.Bool, log *slog.Logger) error {
	conn, err := amqp.Dial(cfg.RabbitURL)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %w", err)
	}
	defer ch.Close()

	if err := ch.Qos(cfg.Prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	q, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, nil)
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

func handle(seen *SeenSet, state *State, msg amqp.Delivery, log *slog.Logger) {
	var event hazard.Event
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		log.Warn("dropping unparsable message", "action", "drop", "err", err, "routing_key", msg.RoutingKey, "correlation_id", msg.CorrelationId)
		_ = msg.Nack(false, false)
		return
	}

	key := event.HazardID + "|" + string(event.Severity)
	if seen.Has(key) {
		log.Info("duplicate skipped", "action", "duplicate", "hazard_id", event.HazardID, "severity", event.Severity, "correlation_id", msg.CorrelationId)
		_ = msg.Ack(false)
		return
	}

	log.Info("received hazard event", "action", "received",
		"hazard_id", event.HazardID, "hazard_type", event.HazardType, "severity", event.Severity,
		"routing_key", msg.RoutingKey, "redelivered", msg.Redelivered, "correlation_id", msg.CorrelationId)

	state.Apply(event)
	seen.Add(key)
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
