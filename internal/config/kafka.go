package config

import "time"

// KafkaConsumerConfig holds configuration for Kafka consumer
type KafkaConsumerConfig struct {
	// Brokers is a list of Kafka broker addresses
	Brokers []string `yaml:"brokers" env:"KAFKA_BROKERS"`

	// Topic is the Kafka topic to consume from
	Topic string `yaml:"topic" env:"KAFKA_TOPIC"`

	// GroupID is the consumer group ID
	GroupID string `yaml:"group_id" env:"KAFKA_GROUP_ID"`

	// BatchSize is the number of messages to consume in a single batch
	BatchSize int `yaml:"batch_size" env:"KAFKA_BATCH_SIZE"`

	// BatchTimeout is the maximum time to wait for a batch to fill up
	BatchTimeout time.Duration `yaml:"batch_timeout" env:"KAFKA_BATCH_TIMEOUT"`

	// SessionTimeout is the consumer group session timeout
	SessionTimeout time.Duration `yaml:"session_timeout" env:"KAFKA_SESSION_TIMEOUT"`

	// RebalanceTimeout is the consumer group rebalance timeout
	RebalanceTimeout time.Duration `yaml:"rebalance_timeout" env:"KAFKA_REBALANCE_TIMEOUT"`

	// StartOffset defines where to start consuming from if no offset is stored.
	// Possible values: "earliest", "latest"
	StartOffset string `yaml:"start_offset" env:"KAFKA_START_OFFSET"`

	// CommitInterval is the interval at which offsets are committed
	CommitInterval time.Duration `yaml:"commit_interval" env:"KAFKA_COMMIT_INTERVAL"`

	// TLS holds optional TLS configuration
	TLS *TLSConfig `yaml:"tls"`

	// SASL holds optional SASL authentication configuration
	SASL *SASLConfig `yaml:"sasl"`
}

// ClickHouseProducerConfig holds configuration for ClickHouse producer
type ClickHouseProducerConfig struct {
	// Addresses is a list of ClickHouse server addresses
	Addresses []string `yaml:"addresses" env:"CLICKHOUSE_ADDRESSES"`

	// Database is the ClickHouse database name
	Database string `yaml:"database" env:"CLICKHOUSE_DATABASE"`

	// Table is the ClickHouse table name to write to
	Table string `yaml:"table" env:"CLICKHOUSE_TABLE"`

	// Username is the ClickHouse username
	Username string `yaml:"username" env:"CLICKHOUSE_USERNAME"`

	// Password is the ClickHouse password
	Password string `yaml:"password" env:"CLICKHOUSE_PASSWORD"`

	// BatchSize is the number of rows to insert in a single batch
	BatchSize int `yaml:"batch_size" env:"CLICKHOUSE_BATCH_SIZE"`

	// FlushInterval is the maximum time to wait before flushing a batch
	FlushInterval time.Duration `yaml:"flush_interval" env:"CLICKHOUSE_FLUSH_INTERVAL"`

	// MaxRetries is the number of retries on insert failure
	MaxRetries int `yaml:"max_retries" env:"CLICKHOUSE_MAX_RETRIES"`

	// RetryDelay is the delay between retries
	RetryDelay time.Duration `yaml:"retry_delay" env:"CLICKHOUSE_RETRY_DELAY"`

	// DialTimeout is the timeout for establishing a connection
	DialTimeout time.Duration `yaml:"dial_timeout" env:"CLICKHOUSE_DIAL_TIMEOUT"`

	// MaxOpenConns is the maximum number of open connections
	MaxOpenConns int `yaml:"max_open_conns" env:"CLICKHOUSE_MAX_OPEN_CONNS"`

	// MaxIdleConns is the maximum number of idle connections
	MaxIdleConns int `yaml:"max_idle_conns" env:"CLICKHOUSE_MAX_IDLE_CONNS"`

	// ConnMaxLifetime is the maximum lifetime of a connection
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime" env:"CLICKHOUSE_CONN_MAX_LIFETIME"`

	// Async enables asynchronous inserts on the ClickHouse side
	Async bool `yaml:"async" env:"CLICKHOUSE_ASYNC"`

	// TLS holds optional TLS configuration
	TLS *TLSConfig `yaml:"tls"`
}

// TLSConfig holds TLS configuration shared between Kafka and ClickHouse
type TLSConfig struct {
	// Enable enables TLS
	Enable bool `yaml:"enable"`

	// CACert is the path to the CA certificate file
	CACert string `yaml:"ca_cert"`

	// ClientCert is the path to the client certificate file
	ClientCert string `yaml:"client_cert"`

	// ClientKey is the path to the client key file
	ClientKey string `yaml:"client_key"`

	// InsecureSkipVerify disables server certificate verification
	InsecureSkipVerify bool `yaml:"insecure_skip_verify"`
}

// SASLConfig holds SASL authentication configuration for Kafka
type SASLConfig struct {
	// Mechanism is the SASL mechanism to use: "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"
	Mechanism string `yaml:"mechanism"`

	// Username is the SASL username
	Username string `yaml:"username"`

	// Password is the SASL password
	Password string `yaml:"password"`
}



// DefaultKafkaConsumerConfig returns a KafkaConsumerConfig with sensible defaults
func DefaultKafkaConsumerConfig() KafkaConsumerConfig {
	return KafkaConsumerConfig{
		BatchSize:        1000,
		BatchTimeout:     5 * time.Second,
		SessionTimeout:   30 * time.Second,
		RebalanceTimeout: 60 * time.Second,
		CommitInterval:   5 * time.Second,
		StartOffset:      "latest",
	}
}

// DefaultClickHouseProducerConfig returns a ClickHouseProducerConfig with sensible defaults
func DefaultClickHouseProducerConfig() ClickHouseProducerConfig {
	return ClickHouseProducerConfig{
		BatchSize:       5000,
		FlushInterval:   10 * time.Second,
		MaxRetries:      3,
		RetryDelay:      1 * time.Second,
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 10 * time.Minute,
		Async:           false,
	}
}
