package ports

import "context"

type Batch[I any] interface {
	Payload() []I
	Commit(context.Context) error
}

type Ingester interface {
	Run(ctx context.Context) error
}

type Consumer[I any] interface {
	Consume(ctx context.Context) (<-chan Batch[I], <-chan error)
}

type Producer[O any] interface {
	Write(ctx context.Context, msg []O) error
}


type Process[I, O any] func(context.Context, []I) ([]O, error)