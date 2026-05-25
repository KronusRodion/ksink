package ingestor

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


func (b *Builder[I, O]) WithKafkaConsumer(client *kgo.Client, batchSize int) {
	cons := consumer.NewKafkaConsumer[I](client, batchSize)
	b.consumer = cons
}

func (b *Builder[I, O]) WithClickHouse(conn clickhouse.Conn, table string, columns []string, mapper producer.RowMapper[O]) {
	b.producer = producer.NewClickHouseProducer(conn, table, columns, mapper)
	
}

func (b *Builder[I, O]) WithHandler(handler ports.Process[I, O]) {
	b.handler = handler
}

func (b *Builder[I, O]) Build() (ingestor[I, O], error) {
	err := b.validate()
	if err != nil {
		return ingestor[I, O]{}, err
	}
	
	return ingestor[I, O]{consumer: b.consumer, Producer: b.producer, handler: b.handler}, nil
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