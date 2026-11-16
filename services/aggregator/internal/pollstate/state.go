package pollstate

import (
	"context"
	"time"
)

type Cursor struct {
	Value     string    `bson:"value"`
	Revision  int64     `bson:"revision"`
	UpdatedAt time.Time `bson:"updated_at"`
}

type Store interface {
	LoadPollCursor(ctx context.Context, source string) (string, error)
	SavePollCursor(ctx context.Context, source, cursor string) error
}

type Unit interface {
	LoadPollCursor(ctx context.Context, source string) (Cursor, error)
	SavePollCursor(ctx context.Context, source string, cursor Cursor) error
}
