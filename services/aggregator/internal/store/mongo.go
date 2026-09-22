package store

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func Connect(uri string) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	// pas mongo.connect, di driver v2 tu gk lgsg tes koneksi, jd ping buat beneran pastiin

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	return client, nil
}
