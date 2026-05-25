package ingestor

type Consumer interface {
}

type Producer interface {
}

type ingestor[I, O any] struct {
	handler  func(I) (O, error)
	consumer Consumer
	Producer Producer
}
