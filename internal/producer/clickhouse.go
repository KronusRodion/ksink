package producer

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// RowMapper maps a generic payload I to a slice of values
// that correspond to the columns in the ClickHouse table.
//
// Example:
//
//	func eventMapper(e Event) ([]any, error) {
//		return []any{e.UserID, e.Action, e.Timestamp}, nil
//	}
type RowMapper[I any] func(I) ([]any, error)

// ClickHouse is a generic producer that writes batches of type I to ClickHouse.
type ClickHouse[I any] struct {
	conn    clickhouse.Conn
	table   string
	columns []string
	mapper  RowMapper[I]
}

func NewClickHouseProducer[I any](
	conn clickhouse.Conn,
	table string,
	columns []string,
	mapper RowMapper[I],
) *ClickHouse[I] {
	return &ClickHouse[I]{
		conn:    conn,
		table:   table,
		columns: columns,
		mapper:  mapper,
	}
}

// Write inserts a batch of messages into ClickHouse in a single batch send.
// If any row fails to map, the entire batch is aborted and the error is returned.
func (c *ClickHouse[I]) Write(ctx context.Context, messages []I) error {
	if len(messages) == 0 {
		return nil
	}

	batch, err := c.conn.PrepareBatch(ctx, c.insertQuery())
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for i, msg := range messages {
		row, err := c.mapper(msg)
		if err != nil {
			// Abort releases the batch resources on the CH side
			_ = batch.Abort()
			return fmt.Errorf("map row %d: %w", i, err)
		}

		if err := batch.Append(row...); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append row %d: %w", i, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	return nil
}

// insertQuery builds the INSERT query string for the configured table and columns.
func (c *ClickHouse[I]) insertQuery() string {
	return fmt.Sprintf("INSERT INTO %s (%s)", c.table, joinColumns(c.columns))
}

func joinColumns(cols []string) string {
	result := ""
	for i, col := range cols {
		if i > 0 {
			result += ", "
		}
		result += col
	}
	return result
}