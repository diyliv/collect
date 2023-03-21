package interfaces

import "context"

type Producer interface {
	Produce(ctx context.Context, message interface{}) error
}
