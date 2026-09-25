package store

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"aggregator/internal/hazard"
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

type HazardStore struct {
	collection *mongo.Collection
}

func NewHazardStore(client *mongo.Client, dbName, collectionName string) *HazardStore {
	return &HazardStore{
		collection: client.Database(dbName).Collection(collectionName),
	}
}

func (s *HazardStore) SaveHazardEvent(event hazard.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := map[string]any{"hazard_id": event.HazardID}
	opts := options.Replace().SetUpsert(true)
	_, err := s.collection.ReplaceOne(ctx, filter, event, opts)
	return err
}

type Filter struct {
	HazardType string
	Severity   string
	Since      *time.Time
}

func (s *HazardStore) GetHazardEvents(limit int64, filter Filter) ([]hazard.Event, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := map[string]any{}
	if filter.HazardType != "" {
		query["hazard_type"] = filter.HazardType
	}
	if filter.Severity != "" {
		query["severity"] = filter.Severity
	}
	if filter.Since != nil {
		query["occurred_at"] = map[string]any{"$gt": *filter.Since}
	}

	opts := options.Find().SetSort(map[string]int{"ingested_at": -1}).SetLimit(limit)
	cursor, err := s.collection.Find(ctx, query, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var events []hazard.Event
	if err := cursor.All(ctx, &events); err != nil {
		return nil, err
	}

	return events, nil
}
