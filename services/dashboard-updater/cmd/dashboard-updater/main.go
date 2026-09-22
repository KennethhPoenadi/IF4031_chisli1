package main

import (
	"encoding/json"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"

	"dashboard-updater/internal/hazard"
)

func main() {
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	exchange := getEnv("RABBITMQ_EXCHANGE", "hazard_events")
	bindingKey := getEnv("BINDING_KEY", "hazard.#")

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("rabbitmq connect failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel open failed: %v", err)
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare(
		"dashboard-updater-queue", // nama queue punya consumer ini
		true, // durable -> tetep ada kalo rabbit direstart
		false, // auto delete kl ga ada yang consume
		false, // exclusive, false biar bisa dipake connection lain juga
		false, // no-wait, tunggu konfirmasi dari server
		nil, // optional argumen tambahan
	)
	if err != nil {
		log.Fatalf("queue declare failed: %v", err)
	}

	err = ch.QueueBind(
		queue.Name, // queue yang mau di bind
		bindingKey, // pola routing key yang mau didengerin, "hazard.#" = semua
		exchange, // exchange asalnya, hazard_events
		false, // no wait biar bs kerja yg lain lg
		nil, // optional argumen tambahan
	) // ini yang nyambungin queue ke exchangenya aggregator
	if err != nil {
		log.Fatalf("queue bind failed: %v", err)
	}

	msgs, err := ch.Consume(
		queue.Name, // queue yang mau dibaca
		"", // consumer tag, kosong biar rabbit generate sendiri
		true, // auto ack, pesan langsung dianggap selesai pas diterima
		false, // exclusive
		false, // no local, ga dipake sama rabbit
		false,  // no wait ini biar bs kerja yg lain lg
		nil,  // optional argumen tambahan
	) // msgs ini channel go yang keisi otomatis tiap ada pesan baru
	if err != nil {
		log.Fatalf("consume failed: %v", err)
	}

	log.Println("dashboard-updater listening on exchange", exchange, "with binding key", bindingKey)

	for msg := range msgs { // loop ini nungguin terus, tiap ada pesan baru langsung diproses
		var event hazard.Event
		if err := json.Unmarshal(msg.Body, &event); err != nil { // ubah balik json jadi struct hazard.Event
			log.Printf("failed to parse message: %v", err)
			continue 
		}
		log.Printf("received hazard event: %s | %s | %s | %s", event.HazardID, event.HazardType, event.Severity, event.AreaName)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
