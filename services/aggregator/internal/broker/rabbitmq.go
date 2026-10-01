package broker

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"aggregator/internal/hazard"
)

func Connect(url, exchange, exchangeType string) (*amqp.Connection, *amqp.Channel, error) {
	var conn *amqp.Connection
	var err error

	for attempt := 1; attempt <= 10; attempt++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second) // rabbitmq mungkin belum bener2 siap walau containernya udah healthy, jd retry
	}
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	err = ch.ExchangeDeclare(
		exchange, // ini nama
		exchangeType, // tipenya apa? bisa topic, fanout, direct, default, dll gtt
		true, // durable -> tetep ada kalo rabbitnye di restart
		false, // auto delete kl ga ada yang cnnect
		false, // boleh nunggu dipake publisher dr luar
		false, // ga wait untuk nungu konfirmasi
		nil, // optional argumen tambahan
	)
	if err != nil { // klo gagal ttp lgsg
		ch.Close()
		conn.Close()
		return nil, nil, err
	}

	return conn, ch, nil
}

func Publish(ch *amqp.Channel, exchange string, event hazard.Event, correlationID string) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	routingKey := strings.ToLower("hazard." + string(event.HazardType) + "." + string(event.Severity)) // contohnya jadi hazard.seismic.awas kalo hazard typenya seismic dan severitynya awas

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/json",
			DeliveryMode:  amqp.Persistent,
			CorrelationId: correlationID,
			Body:          body,
		},
	) // bwt ngirimnya aj
}
