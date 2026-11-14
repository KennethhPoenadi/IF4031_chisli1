package coastalstate

import (
	"context"
	"time"
)

type Cursor struct {
	Value     string    `bson:"value"`
	Revision  int64     `bson:"revision"`
	UpdatedAt time.Time `bson:"updated_at"`
}

type Rejection struct {
	Observation   map[string]any `bson:"observation"`
	Reason        string         `bson:"reason"`
	CorrelationID string         `bson:"correlation_id"`
	RejectedAt    time.Time      `bson:"rejected_at"`
}

type Store interface {
	LoadCursor(ctx context.Context) (string, error)
	SaveCursor(ctx context.Context, cursor string) error
	Quarantine(ctx context.Context, rejection Rejection) error
}

type Unit interface {
	LoadCoastalCursor(ctx context.Context) (Cursor, error)
	SaveCoastalCursor(ctx context.Context, cursor Cursor) error
	QuarantineCoastal(ctx context.Context, rejection Rejection) error
}
