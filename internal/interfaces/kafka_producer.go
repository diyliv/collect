package interfaces

import (
	"context"
	"time"
)

type Producer interface {
	Produce(ctx context.Context, tagName string, itemQuality int16, readAt time.Time, message interface{}) error
}
