package ingester

import (
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/KronusRodion/ksink/internal/consumer"
	"github.com/KronusRodion/ksink/internal/ports"
	"github.com/KronusRodion/ksink/internal/producer"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Builder[I, O any] struct {
	// Kafka
	consumer ports.Consumer[I]
	// Clickhouse
	producer ports.Producer[O]
	//Handler
	handler ports.Process[I, O]
}

func (b *Builder[I, O]) WithKafkaConsumer(client *kgo.Client, batchSize int) *Builder[I, O] {
	cons := consumer.NewKafkaConsumer[I](client, batchSize)
	b.consumer = cons
	return b
}

func (b *Builder[I, O]) WithClickHouse(conn clickhouse.Conn, table string, columns []string, mapper producer.RowMapper[O]) *Builder[I, O] {
	b.producer = producer.NewClickHouseProducer(conn, table, columns, mapper)
	return b
}

func (b *Builder[I, O]) WithHandler(handler ports.Process[I, O]) *Builder[I, O] {
	b.handler = handler
	return b
}

func (b *Builder[I, O]) Build() (ingester[I, O], error) {
	err := b.validate()
	if err != nil {
		return ingester[I, O]{}, err
	}

	return ingester[I, O]{consumer: b.consumer, producer: b.producer, handler: b.handler}, nil
}

func (b *Builder[I, O]) validate() error {
	if b.consumer == nil {
		return errors.New("nil consumer")
	}

	if b.producer == nil {
		return errors.New("nil producer")
	}

	if b.handler == nil {
		return errors.New("nil handler")
	}

	return nil
}
