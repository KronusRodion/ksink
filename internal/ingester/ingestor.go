package ingester

import (
	"context"
	"log"

	"github.com/KronusRodion/ksink/internal/ports"
)

type ingester[I, O any] struct {
	handler  ports.Process[I, O]
	consumer ports.Consumer[I]
	producer ports.Producer[O]
}

func Newingester[I, O any](consumer ports.Consumer[I], producer ports.Producer[O], handler ports.Process[I, O]) ingester[I, O] {
	return ingester[I, O]{consumer: consumer, producer: producer, handler: handler}
}

func (i ingester[I, O]) Run(ctx context.Context) error {

	batchs, errs := i.consumer.Consume(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errs:
			return err
		case batch, ok := <-batchs:
			if !ok {
				return nil
			}

			data, err := i.handler(ctx, batch.Payload())
			if err != nil {
				log.Println("error producing data: ", err)
			}

			err = i.producer.Write(ctx, data)
			if err != nil {
				log.Println("error producing data: ", err)
			}

			err = batch.Commit(ctx)
			if err != nil {
				log.Println("error commiting data: ", err)
			}
		}
	}
}
