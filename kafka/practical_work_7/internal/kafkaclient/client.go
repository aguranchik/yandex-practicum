package kafkaclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Config struct {
	Brokers       []string
	Username      string
	Password      string
	CAFile        string
	ConsumerGroup string
	Topic         string
}

func NewProducer(cfg Config) (*kgo.Client, error) {
	tlsConfig, err := loadTLSConfig(cfg.CAFile)
	if err != nil {
		return nil, err
	}

	return kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DialTLSConfig(tlsConfig),
		kgo.SASL(scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha512Mechanism()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)
}

func NewConsumer(cfg Config) (*kgo.Client, error) {
	tlsConfig, err := loadTLSConfig(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	if cfg.ConsumerGroup == "" {
		return nil, fmt.Errorf("consumer group is required")
	}

	return kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DialTLSConfig(tlsConfig),
		kgo.SASL(scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha512Mechanism()),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
}

func loadTLSConfig(caFile string) (*tls.Config, error) {
	certificate, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA certificate %q: %w", caFile, err)
	}

	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(certificate) {
		return nil, fmt.Errorf("CA file %q does not contain a PEM certificate", caFile)
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
	}, nil
}
