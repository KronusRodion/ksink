package clients

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/KronusRodion/ksink/internal/config"
)

// NewClickHouseClient creates a new ClickHouse client from the given config
func NewClickHouseClient(cfg config.ClickHouseProducerConfig) (clickhouse.Conn, error) {
	if err := validateClickHouseConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid clickhouse config: %w", err)
	}

	options := &clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     cfg.DialTimeout,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		Settings: clickhouse.Settings{
			"async_insert": boolToInt(cfg.Async),
		},
	}

	// TLS
	if cfg.TLS != nil && cfg.TLS.Enable {
		tlsCfg, err := buildTLSConfig(cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("clickhouse tls: %w", err)
		}
		options.TLS = tlsCfg
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse connection: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	return conn, nil
}



func validateClickHouseConfig(cfg config.ClickHouseProducerConfig) error {
	if len(cfg.Addresses) == 0 {
		return fmt.Errorf("addresses must not be empty")
	}
	if cfg.Database == "" {
		return fmt.Errorf("database must not be empty")
	}
	if cfg.Table == "" {
		return fmt.Errorf("table must not be empty")
	}
	if cfg.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be > 0")
	}
	if cfg.FlushInterval <= 0 {
		return fmt.Errorf("flush_interval must be > 0")
	}
	return nil
}
