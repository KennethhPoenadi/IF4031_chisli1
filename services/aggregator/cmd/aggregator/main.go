package main

import (
	"log"
	"net/http"
	"time"

	"aggregator/internal/api"
	"aggregator/internal/broker"
	"aggregator/internal/config"
	"aggregator/internal/hazard"
	"aggregator/internal/store"

	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load(".env", "../../../.env")

	cfg := config.Load()

	mongoClient, err := store.Connect(cfg.MongoURI)
	if err != nil {
		log.Fatalf("mongo connect failed: %v", err)
	}
	log.Println("connected to mongo")
	defer mongoClient.Disconnect(nil) // ini bwt nutup klo nnti koneksi mati

	hazardStore := store.NewHazardStore(mongoClient, cfg.MongoDBName, cfg.MongoHazardCollection)

	rabbitConn, rabbitCh, err := broker.Connect(cfg.RabbitMQURL, cfg.RabbitMQExchange, cfg.RabbitMQExchangeType)
	if err != nil {
		log.Fatalf("rabbitmq connect failed: %v", err)
	}
	log.Println("connected to rabbitmq, exchange declared:", cfg.RabbitMQExchange)
	defer rabbitCh.Close() // ini bwt nutup klo nnti koneksi mati
	defer rabbitConn.Close() // ini bwt nutup klo nnti koneksi mati

	dummyEvent := hazard.Event{
		HazardID:    "test-hz-001",
		Source:      hazard.SourceBMKG,
		SourceRefID: "test-event-001",
		HazardType:  hazard.TypeSeismic,
		Severity:    hazard.SeverityWaspada,
		AreaName:    "Sukabumi",
		Latitude:    -7.02,
		Longitude:   106.56,
		OccurredAt:  time.Now(),
		IngestedAt:  time.Now(),
		Attributes: map[string]any{
			"magnitude": 5.4,
			"depth_km":  10.2,
		},
	}
	if err := hazardStore.SaveHazardEvent(dummyEvent); err != nil {
		log.Printf("failed to save dummy event: %v", err)
	} else {
		log.Println("saved dummy hazard event to mongo")
	}

	if err := broker.Publish(rabbitCh, cfg.RabbitMQExchange, dummyEvent); err != nil {
		log.Printf("failed to publish dummy event: %v", err)
	} else {
		log.Println("published dummy hazard event")
	}

	handler := api.NewHandler(hazardStore)

	//cek health
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/hazard-events", handler.GetHazardEvents)

	log.Println("aggregator listening on :" + cfg.AggregatorPort)
	if err := http.ListenAndServe(":"+cfg.AggregatorPort, mux); err != nil {
		log.Fatal(err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}
