package ingestor

import (
	"context"
	"log"

	"github.com/KronusRodion/ksink/internal/ports"
)

type ingestor[I, O any] struct {
	handler  ports.Process[I, O]
	consumer ports.Consumer[I]
	Producer ports.Producer[O]
}

func (i ingestor[I, O]) Run(ctx context.Context) error {

	batchs, errs := i.consumer.Consume(ctx)


	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <- errs:
			return err
		case batch, ok := <-batchs:
			if !ok {
				return nil
			}

			data, err := i.handler(ctx, batch.Payload())
			if err != nil {
				log.Println("error producing data: ", err)
			}

			err = i.Producer.Write(ctx, data)
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
