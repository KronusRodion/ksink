package ksink

import (
	"github.com/KronusRodion/ksink/internal/ingester"
	"github.com/KronusRodion/ksink/internal/ports"
)

func NewBuilder[I, O any]() ingester.Builder[I, O] {
	return ingester.Builder[I, O]{}
}

func NewIngester[I, O any](consumer ports.Consumer[I], producer ports.Producer[O], handler ports.Process[I, O]) ports.Ingester {
	return ingester.Newingester(consumer, producer, handler)
}
