package amqpx

import (
	"context"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DLXExchange = "hazard_events.dlx"
	DLQQueue = "hazard_events.dlq"
	FieldReportsQueue = "field_reports"
)


func QueueArgs() amqp.Table { return amqp.Table{"x-dead-letter-exchange": DLXExchange} }

// mendeklarasikan exchange dan queue dead-letter.
func DeclareDLX(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(DLXExchange, "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(DLQQueue, true, false, false, false, nil); err != nil {
		return err
	}
	return ch.QueueBind(DLQQueue, "", DLXExchange, false, nil)
}

// mendeklarasikan queue laporan Tim Lapangan (durable, ber-DLX).
func DeclareFieldReports(ch *amqp.Channel) error {
	if err := DeclareDLX(ch); err != nil {
		return err
	}
	_, err := ch.QueueDeclare(FieldReportsQueue, true, false, false, false, QueueArgs())
	return err
}

type Setup func(ch *amqp.Channel) (<-chan amqp.Delivery, error)

// menjalankan konsumsi sampai ctx selesai, tersambung ulang dengan backoff bila putus.
func Consume(ctx context.Context, url, name string, log *slog.Logger, setup Setup, handle func(context.Context, amqp.Delivery)) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := consumeOnce(ctx, url, setup, handle)
		if ctx.Err() != nil {
			return
		}
		log.Warn("konsumer terputus, menyambung ulang", "consumer", name, "err", err, "backoff_ms", backoff.Milliseconds())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 15*time.Second)
	}
}

func consumeOnce(ctx context.Context, url string, setup Setup, handle func(context.Context, amqp.Delivery)) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	deliveries, err := setup(ch)
	if err != nil {
		return err
	}
	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-closed:
			if e == nil {
				return amqp.ErrClosed
			}
			return e
		case d, ok := <-deliveries:
			if !ok {
				return amqp.ErrClosed
			}
			handle(ctx, d)
		}
	}
}

// mengembalikan nama queue asal pesan yang di-dead-letter (dari header x-death).
func DeathQueue(h amqp.Table) (queue, reason string) {
	deaths, ok := h["x-death"].([]any)
	if !ok || len(deaths) == 0 {
		return "", ""
	}
	first, ok := deaths[0].(amqp.Table)
	if !ok {
		return "", ""
	}
	queue, _ = first["queue"].(string)
	reason, _ = first["reason"].(string)
	return queue, reason
}
