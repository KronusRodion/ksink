package clients

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/KronusRodion/ksink/internal/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// NewKafkaClient creates a new Kafka consumer client from the given config
func NewKafkaClient(cfg config.KafkaConsumerConfig) (*kgo.Client, error) {
	if err := validateKafkaConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid kafka config: %w", err)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.RebalanceTimeout(cfg.RebalanceTimeout),
		kgo.AutoCommitInterval(cfg.CommitInterval),
	}

	// Start offset
	switch cfg.StartOffset {
	case "earliest":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	case "latest", "":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	default:
		return nil, fmt.Errorf("unknown start_offset %q: use 'earliest' or 'latest'", cfg.StartOffset)
	}

	// TLS
	if cfg.TLS != nil && cfg.TLS.Enable {
		tlsCfg, err := buildTLSConfig(cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("kafka tls: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	// SASL
	if cfg.SASL != nil {
		saslOpt, err := buildSASLOpt(cfg.SASL)
		if err != nil {
			return nil, fmt.Errorf("kafka sasl: %w", err)
		}
		opts = append(opts, saslOpt)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	return client, nil
}

// buildTLSConfig creates a tls.Config from TLSConfig
func buildTLSConfig(cfg *config.TLSConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	// CA cert
	if cfg.CACert != "" {
		caCert, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("read ca cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("parse ca cert: invalid PEM")
		}
		tlsCfg.RootCAs = pool
	}

	// Client cert + key (mutual TLS)
	if cfg.ClientCert != "" && cfg.ClientKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// buildSASLOpt creates a kgo.Opt for the given SASL config
func buildSASLOpt(cfg *config.SASLConfig) (kgo.Opt, error) {
	switch cfg.Mechanism {
	case "PLAIN":
		return kgo.SASL(plain.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsMechanism()), nil

	case "SCRAM-SHA-256":
		return kgo.SASL(scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsSha256Mechanism()), nil

	case "SCRAM-SHA-512":
		return kgo.SASL(scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsSha512Mechanism()), nil

	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q: use PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512", cfg.Mechanism)
	}
}

// --- Validation ---

func validateKafkaConfig(cfg config.KafkaConsumerConfig) error {
	if len(cfg.Brokers) == 0 {
		return fmt.Errorf("brokers must not be empty")
	}
	if cfg.Topic == "" {
		return fmt.Errorf("topic must not be empty")
	}
	if cfg.GroupID == "" {
		return fmt.Errorf("group_id must not be empty")
	}
	if cfg.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be > 0")
	}
	if cfg.BatchTimeout <= 0 {
		return fmt.Errorf("batch_timeout must be > 0")
	}
	if cfg.SASL != nil && cfg.SASL.Mechanism == "PLAIN" && (cfg.TLS == nil || !cfg.TLS.Enable) {
		return fmt.Errorf("SASL PLAIN requires TLS to be enabled")
	}
	return nil
}



// --- Helpers ---

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
