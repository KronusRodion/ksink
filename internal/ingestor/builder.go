package ingestor

import (
	"github.com/KronusRodion/ksink/internal/clients"
	"github.com/KronusRodion/ksink/internal/config"
	"github.com/KronusRodion/ksink/internal/consumer"
	"github.com/KronusRodion/ksink/internal/producer"
)

type Builder[I, O any] struct {
	// Kafka
	kafkaCfg config.KafkaConsumerConfig

	// Clickhouse
	clickCfg config.ClickHouseProducerConfig
	columns  []string
	mapper   producer.RowMapper[O]

	//Handler
	handler func(I) (O, error)
}

func (b *Builder[I, O]) WithKafka(kafkaCfg config.KafkaConsumerConfig) {
	b.kafkaCfg = kafkaCfg
}

func (b *Builder[I, O]) WithClickHouse(clickCfg config.ClickHouseProducerConfig, columns []string, mapper producer.RowMapper[O]) {
	b.clickCfg = clickCfg
	b.columns = columns
	b.mapper = mapper
}

func (b *Builder[I, O]) WithHandler(clickCfg config.ClickHouseProducerConfig) {
	b.clickCfg = clickCfg
}

func (b *Builder[I, O]) Build() (ingestor[I, O], error) {
	kafka, err := clients.NewKafkaClient(b.kafkaCfg)
	if err != nil {
		return ingestor[I, O]{}, err
	}

	click, err := clients.NewClickHouseClient(b.clickCfg)
	if err != nil {
		return ingestor[I, O]{}, err
	}

	consumer := consumer.NewKafkaConsumer[I](kafka, b.clickCfg.BatchSize)
	producer := producer.NewClickHouseProducer(click, b.clickCfg.Table, b.columns, b.mapper)

	return ingestor[I, O]{consumer: consumer, Producer: producer, handler: b.handler}, nil
}
