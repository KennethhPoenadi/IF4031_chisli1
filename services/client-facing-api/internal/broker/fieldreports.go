package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"telemetry"
)

const (
	// Harus sama dengan deklarasi di Aggregator (internal/amqpx), termasuk argumennya.
	dlxExchange       = "hazard_events.dlx"
	dlqQueue          = "hazard_events.dlq"
	FieldReportsQueue = "field_reports"
)

type Publisher struct {
	url  string
	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

func New(url string) *Publisher { return &Publisher{url: url} }

func (p *Publisher) connectLocked() error {
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}
	declare := func() error {
		if err := ch.ExchangeDeclare(dlxExchange, "fanout", true, false, false, false, nil); err != nil {
			return err
		}
		if _, err := ch.QueueDeclare(dlqQueue, true, false, false, false, nil); err != nil {
			return err
		}
		if err := ch.QueueBind(dlqQueue, "", dlxExchange, false, nil); err != nil {
			return err
		}
		_, err := ch.QueueDeclare(FieldReportsQueue, true, false, false, false, amqp.Table{"x-dead-letter-exchange": dlxExchange})
		if err != nil {
			return err
		}
		return ch.Confirm(false)
	}
	if err := declare(); err != nil {
		conn.Close()
		return err
	}
	p.conn, p.ch = conn, ch
	return nil
}

func (p *Publisher) closeLocked() {
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.conn, p.ch = nil, nil
}

func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeLocked()
}

// dipakai untuk kesiapan pod.
func (p *Publisher) Connected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ch != nil && !p.ch.IsClosed()
}

// menerbitkan satu laporan dan menunggu konfirmasi broker.
// Mengembalikan nil hanya bila broker sudah menyimpan pesannya.
func (p *Publisher) PublishFieldReport(ctx context.Context, reportID string, body []byte, correlationID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch == nil || p.ch.IsClosed() {
		p.closeLocked()
		if err := p.connectLocked(); err != nil {
			return fmt.Errorf("rabbitmq connect: %w", err)
		}
	}
	headers := amqp.Table{}
	ctx, end := telemetry.StartProducerSpan(ctx, "publish "+FieldReportsQueue, headers,
		"messaging.system", "rabbitmq", "messaging.destination.name", FieldReportsQueue, "field_report.id", reportID)
	var spanErr error
	defer func() { end(spanErr) }()

	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, "", FieldReportsQueue, true, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		CorrelationId: correlationID,
		MessageId:     reportID,
		Headers:       headers,
		Body:          body,
	})
	if err != nil {
		p.closeLocked()
		spanErr = fmt.Errorf("rabbitmq publish: %w", err)
		return spanErr
	}
	acked, err := conf.WaitContext(ctx)
	if err != nil {
		p.closeLocked()
		spanErr = fmt.Errorf("rabbitmq confirm: %w", err)
		return spanErr
	}
	if !acked {
		spanErr = errors.New("rabbitmq menolak laporan (nack)")
		return spanErr
	}
	return nil
}
