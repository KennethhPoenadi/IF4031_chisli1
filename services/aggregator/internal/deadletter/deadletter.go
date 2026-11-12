package deadletter

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
	"unicode/utf8"

	amqp "github.com/rabbitmq/amqp091-go"

	"aggregator/internal/amqpx"
	"aggregator/internal/dlq"
	"aggregator/internal/hazard"
)

type Recorder interface {
	Record(ctx context.Context, e dlq.Entry) error
}

type Consumer struct {
	dead Recorder
	log  *slog.Logger
}

func New(dead Recorder, log *slog.Logger) *Consumer { return &Consumer{dead: dead, log: log} }

// menebak sumber pesan untuk filter Audit API tanpa mempercayai isinya.
func sourceOf(queue string, body []byte) string {
	if queue == amqpx.FieldReportsQueue {
		return "FIELD_REPORT"
	}
	if utf8.Valid(body) {
		var probe struct {
			Source string `json:"source"`
		}
		if json.Unmarshal(body, &probe) == nil {
			for _, s := range hazard.ValidSources {
				if string(s) == probe.Source {
					return probe.Source
				}
			}
		}
	}
	return "UNKNOWN"
}

func (c *Consumer) Entry(d amqp.Delivery) dlq.Entry {
	queue, why := amqpx.DeathQueue(d.Headers)
	if queue == "" {
		queue = d.RoutingKey
	}
	e := dlq.Entry{
		Source:        sourceOf(queue, d.Body),
		Kind:          dlq.KindAMQP,
		Status:        dlq.StatusParked,
		Reason:        "consumer_rejected: queue=" + queue + " reason=" + why,
		CorrelationID: d.CorrelationId,
		Origin:        queue,
		ReceivedAt:    time.Now().UTC(),
	}
	if !d.Timestamp.IsZero() {
		e.ReceivedAt = d.Timestamp.UTC()
	}
	e.SetPayload(d.Body)
	if ref := refOf(d.Body); ref != "" {
		e.SourceRefID = ref
	}
	return e
}

func refOf(body []byte) string {
	if !utf8.Valid(body) {
		return ""
	}
	var probe struct {
		SourceRefID string `json:"source_ref_id"`
		ReportID    string `json:"report_id"`
	}
	if json.Unmarshal(body, &probe) != nil {
		return ""
	}
	if probe.SourceRefID != "" {
		return probe.SourceRefID
	}
	return probe.ReportID
}

func (c *Consumer) Handle(ctx context.Context, d amqp.Delivery) {
	e := c.Entry(d)
	if err := c.dead.Record(ctx, e); err != nil {
		c.log.Warn("pesan dead-letter belum tersimpan, dicoba lagi", "err", err)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
		}
		_ = d.Nack(false, true)
		return
	}
	c.log.Warn("pesan ditolak konsumer disimpan di DLQ", "source", e.Source, "origin", e.Origin, "bytes", e.PayloadBytes, "correlation_id", e.CorrelationID)
	_ = d.Ack(false)
}

func (c *Consumer) Run(ctx context.Context, url string) {
	amqpx.Consume(ctx, url, "dead-letter", c.log, func(ch *amqp.Channel) (<-chan amqp.Delivery, error) {
		if err := amqpx.DeclareDLX(ch); err != nil {
			return nil, err
		}
		if err := ch.Qos(20, 0, false); err != nil {
			return nil, err
		}
		return ch.Consume(amqpx.DLQQueue, "", false, false, false, false, nil)
	}, c.Handle)
}
