package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"

	"dashboard-updater/internal/config"
	"dashboard-updater/internal/hazard"
)

func Run(ctx context.Context, cfg config.Config, seen *SeenSet, connected *atomic.Bool) error {
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
	fmt.Println("dashboard-updater listening on exchange", cfg.Exchange, "queue", q.Name, "binding key", cfg.BindingKey)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return errors.New("delivery channel closed")
			}
			handle(seen, msg)
		}
	}
}

func handle(seen *SeenSet, msg amqp.Delivery) {
	var event hazard.Event
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		fmt.Println("dropping unparsable message:", err)
		_ = msg.Nack(false, false)
		return
	}

	key := event.HazardID + "|" + string(event.Severity)
	if seen.Has(key) {
		fmt.Printf("duplicate skipped: %s | %s\n", event.HazardID, event.Severity)
		_ = msg.Ack(false)
		return
	}

	fmt.Printf("received hazard event: %s | %s | %s | %s | redelivered=%v\n",
		event.HazardID, event.HazardType, event.Severity, event.AreaName, msg.Redelivered)

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
