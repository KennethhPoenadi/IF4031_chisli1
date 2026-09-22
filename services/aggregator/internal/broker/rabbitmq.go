package broker

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func Connect(url, exchange, exchangeType string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
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
