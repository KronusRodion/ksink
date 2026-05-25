package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Batch holds a decoded batch of messages with a single commit for the whole batch.
type Batch[Payload any] struct {
	Messages   []Payload
	commitFunc func(ctx context.Context) error
}

// Commit marks the entire batch as processed and commits offsets for all messages.
// Must be called after successful ClickHouse write.
func (b *Batch[I]) Commit(ctx context.Context) error {
	return b.commitFunc(ctx)
}

type Kafka[Output any] struct {
	client    *kgo.Client
	batchSize int
}

func NewKafkaConsumer[Output any](client *kgo.Client, batchSize int) *Kafka[Output] {
	return &Kafka[Output]{
		client:    client,
		batchSize: batchSize,
	}
}

// Consume polls Kafka and sends decoded batches into the returned channel.
// Caller MUST call batch.Commit() after successfully processing of batch.
func (k *Kafka[I]) Consume(ctx context.Context) (<-chan *Batch[I], <-chan error) {
	out := make(chan *Batch[I], 1)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			fetches := k.client.PollRecords(ctx, k.batchSize)

			if fetches.IsClientClosed() {
				return
			}

			if errs := fetches.Errors(); len(errs) > 0 {
				for _, e := range errs {
					if e.Err == context.Canceled || e.Err == context.DeadlineExceeded {
						return
					}
					errc <- fmt.Errorf("fetch error topic=%s partition=%d: %w", e.Topic, e.Partition, e.Err)
				}
				return
			}

			batch, err := k.buildBatch(fetches)
			if err != nil {
				errc <- err
				return
			}

			// Empty poll (e.g. timeout with no new messages) — just continue
			if len(batch.Messages) == 0 {
				continue
			}

			select {
			case out <- batch:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, errc
}

// buildBatch decodes all records from fetches into a single Batch.
// The commit function calls MarkCommitted on every record in the batch,
// franz-go will then flush them together on the next CommitInterval tick.
func (k *Kafka[I]) buildBatch(fetches kgo.Fetches) (*Batch[I], error) {
	// Pre-allocate based on actual number of fetched records
	records := make([]*kgo.Record, 0, k.batchSize)
	messages := make([]I, 0, k.batchSize)

	var decodeErr error

	fetches.EachRecord(func(record *kgo.Record) {
		// Stop processing if we already have a decode error
		if decodeErr != nil {
			return
		}

		var payload I
		if err := json.Unmarshal(record.Value, &payload); err != nil {
			decodeErr = fmt.Errorf("unmarshal record offset=%d: %w", record.Offset, err)
			return
		}

		messages = append(messages, payload)
		records = append(records, record)
	})

	if decodeErr != nil {
		return nil, decodeErr
	}

	batch := &Batch[I]{
		Messages: messages,
		commitFunc: func(ctx context.Context) error {
			return k.client.CommitRecords(ctx, records...)

		},
	}

	return batch, nil
}
