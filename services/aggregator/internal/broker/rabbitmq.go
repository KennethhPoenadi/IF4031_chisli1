package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"aggregator/internal/hazard"
	"telemetry"
)

const publishTimeout = 5 * time.Second

type Publisher struct {
	url          string
	exchange     string
	exchangeType string

	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewPublisher(url, exchange, exchangeType string) (*Publisher, error) {
	p := &Publisher{url: url, exchange: exchange, exchangeType: exchangeType}

	var err error
	for attempt := 1; attempt <= 10; attempt++ {
		p.mu.Lock()
		err = p.connectLocked()
		p.mu.Unlock()
		if err == nil {
			return p, nil
		}
		time.Sleep(2 * time.Second)
	}
	return nil, err
}

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
	if err := ch.ExchangeDeclare(p.exchange, p.exchangeType, true, false, false, false, nil); err != nil {
		conn.Close()
		return err
	}
	if err := ch.Confirm(false); err != nil {
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

func (p *Publisher) Connected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ch != nil && !p.ch.IsClosed()
}

func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeLocked()
}

func (p *Publisher) Publish(ctx context.Context, event hazard.Event, correlationID string) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	routingKey := strings.ToLower("hazard." + string(event.HazardType) + "." + string(event.Severity)) // contoh: hazard.seismic.awas

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch == nil || p.ch.IsClosed() {
		p.closeLocked()
		if err := p.connectLocked(); err != nil {
			return fmt.Errorf("rabbitmq reconnect: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	// Konteks trace ikut dikirim lewat header pesan, sehingga pemrosesan di notifier dan dashboard-updater
	// tersambung ke trace yang sama dengan pengambilan data di aggregator.
	headers := amqp.Table{}
	ctx, endSpan := telemetry.StartProducerSpan(ctx, "publish "+p.exchange, headers,
		"messaging.system", "rabbitmq", "messaging.destination.name", p.exchange,
		"messaging.rabbitmq.destination.routing_key", routingKey, "hazard.id", event.HazardID)
	var spanErr error
	defer func() { endSpan(spanErr) }()

	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, p.exchange, routingKey, false, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		CorrelationId: correlationID,
		Headers:       headers,
		Body:          body,
	})
	if err != nil {
		p.closeLocked()
		spanErr = fmt.Errorf("rabbitmq publish: %w", err)
		return spanErr
	}
	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		p.closeLocked()
		spanErr = fmt.Errorf("rabbitmq confirm: %w", err)
		return spanErr
	}
	if !acked {
		spanErr = errors.New("rabbitmq nacked the message")
		return spanErr
	}
	return nil
}
