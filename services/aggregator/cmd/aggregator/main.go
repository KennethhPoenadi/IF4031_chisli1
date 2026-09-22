package main

import (
	"log"
	"net/http"

	"aggregator/internal/broker"
	"aggregator/internal/config"
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

	rabbitConn, rabbitCh, err := broker.Connect(cfg.RabbitMQURL, cfg.RabbitMQExchange, cfg.RabbitMQExchangeType)
	if err != nil {
		log.Fatalf("rabbitmq connect failed: %v", err)
	}
	log.Println("connected to rabbitmq, exchange declared:", cfg.RabbitMQExchange)
	defer rabbitCh.Close() // ini bwt nutup klo nnti koneksi mati
	defer rabbitConn.Close() // ini bwt nutup klo nnti koneksi mati

	//cek health 
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)

	log.Println("aggregator listening on :" + cfg.AggregatorPort)
	if err := http.ListenAndServe(":"+cfg.AggregatorPort, mux); err != nil {
		log.Fatal(err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}
