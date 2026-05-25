package ksink

import "github.com/KronusRodion/ksink/internal/ingestor"

func NewBuilder[I, O any]() ingestor.Builder[I, O] {
	return ingestor.Builder[I, O]{}
}

