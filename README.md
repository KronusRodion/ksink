📖 Ksink is an effective pattern for creating an intermediate layer between a message broker (e.g. Kafka) and OLAP databases (e.g. Clickhouse).

It's often necessary to write a service that enriches the context of a message or modifies it before storing it in the database. This pattern allows for unifying and simplifying the creation of such microservices.

Fast start:
```Go
package main

import (
	"context"

	"github.com/KronusRodion/ksink"
	"github.com/google/uuid"
)

func main() {
	columns := []string{"user_id", "name"}
	batchSize := 10_000

	builder := ksink.NewBuilder[RawKafkaRow, ClickPreparedData]()

	mapper := func(msg ClickPreparedData) ([]any, error) {
		return []any{msg.UserID, msg.Name}, nil
	}

	builder.WithKafkaConsumer(nil, batchSize) // instead of nil put kafka consumer extends ports.Consumer
	builder.WithClickHouse(nil, "user_actions_example", columns, mapper) // instead of nil put click producer extends ports.Producer

	builder.WithHandler(func(ctx context.Context, rkr []RawKafkaRow) ([]ClickPreparedData, error) {
		res := make([]ClickPreparedData, len(rkr))
		for i, v := range rkr {
			res[i] = ClickPreparedData{
				UserID: v.UserID,
				Name:   "John:" + v.UserID.String(), // for example, your service can switch data types to id types from Postgres or adding new useful fields
			}
		}
		return res, nil
	})

	ingester, _ := builder.Build()

	_ = ingester.Run(context.Background())
}

type RawKafkaRow struct {
	UserID uuid.UUID
}

type ClickPreparedData struct {
	UserID uuid.UUID
	Name   string // field that will be add by ingestion
}
```

Also you can create ingestion by NewIngester func passing the parameters right away:
```Go
    ingester := ksink.NewIngester(consumer, producer, handler)
```