package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"

	"situation-analyst/internal/hazard"
	"telemetry"
)

type Observer interface{ Observe(hazard.Event) }

type Config struct {
	URL       string
	Exchange  string
	Queue     string
	Binding   string
	Prefetch  int
	MaxLength int
}

var messages = telemetry.Counter("analyst_messages", "pesan RabbitMQ yang diproses situation-analyst, per hasil")

func QueueArgs(maxLength int) amqp.Table {
	return amqp.Table{"x-max-length": int32(maxLength), "x-overflow": "drop-head"}
}

func Run(ctx context.Context, cfg Config, obs Observer, connected *atomic.Bool, log *slog.Logger) error {
	conn, err := amqp.Dial(cfg.URL)
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
	q, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, QueueArgs(cfg.MaxLength))
	if err != nil {
		return fmt.Errorf("queue declare: %w", err)
	}
	if err := ch.QueueBind(q.Name, cfg.Binding, cfg.Exchange, false, nil); err != nil {
		return fmt.Errorf("queue bind: %w", err)
	}
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	connected.Store(true)
	defer connected.Store(false)
	log.Info("situation-analyst listening", "exchange", cfg.Exchange, "queue", q.Name, "max_length", cfg.MaxLength)

	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-closed:
			return fmt.Errorf("koneksi ditutup: %v", e)
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("saluran konsumsi ditutup")
			}
			Handle(obs, msg, log)
		}
	}
}

type Acker interface {
	Ack(multiple bool) error
	Nack(multiple, requeue bool) error
}

type Message struct {
	Body          []byte
	Headers       map[string]any
	RoutingKey    string
	CorrelationID string
	Acker         Acker
}

func Handle(obs Observer, d amqp.Delivery, log *slog.Logger) {
	HandleMessage(obs, Message{Body: d.Body, Headers: d.Headers, RoutingKey: d.RoutingKey, CorrelationID: d.CorrelationId, Acker: d}, log)
}

func HandleMessage(obs Observer, m Message, log *slog.Logger) {
	ctx, endSpan := telemetry.StartConsumerSpan(context.Background(), "process hazard event", m.Headers,
		"messaging.system", "rabbitmq", "messaging.rabbitmq.destination.routing_key", m.RoutingKey,
		"correlation.id", m.CorrelationID)
	outcome := "observed"
	defer func() {
		endSpan(nil)
		messages(ctx, 1, "outcome", outcome)
	}()

	var ev hazard.Event
	if err := json.Unmarshal(m.Body, &ev); err != nil {
		outcome = "dropped"
		log.Warn("pesan tidak terbaca dibuang", "routing_key", m.RoutingKey, "correlation_id", m.CorrelationID, "err", err.Error())
		_ = m.Acker.Nack(false, false)
		return
	}
	obs.Observe(ev)
	_ = m.Acker.Ack(false)
}
