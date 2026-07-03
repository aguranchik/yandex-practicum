package kafkautil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type ClientConfig struct {
	Brokers  []string
	Username string
	Password string
	CAFile   string
	ClientID string
}

func FromEnv(defaultBrokers, defaultUsername, defaultPassword, clientID string) (ClientConfig, error) {
	return FromPrefixedEnv("", defaultBrokers, defaultUsername, defaultPassword, clientID)
}

func FromPrefixedEnv(prefix, defaultBrokers, defaultUsername, defaultPassword, clientID string) (ClientConfig, error) {
	brokers := splitCSV(envOrDefault(prefix+"KAFKA_BROKERS", defaultBrokers))
	if len(brokers) == 0 {
		return ClientConfig{}, fmt.Errorf("%sKAFKA_BROKERS must contain at least one broker", prefix)
	}

	return ClientConfig{
		Brokers:  brokers,
		Username: envOrDefault(prefix+"KAFKA_USERNAME", defaultUsername),
		Password: envOrDefault(prefix+"KAFKA_PASSWORD", defaultPassword),
		CAFile:   envOrDefault(prefix+"KAFKA_CA_FILE", "/etc/kafka/ca/ca.crt"),
		ClientID: clientID,
	}, nil
}

func NewSaramaConfig(client ClientConfig) (*sarama.Config, error) {
	caPEM, err := os.ReadFile(client.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read Kafka CA file: %w", err)
	}

	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("Kafka CA file does not contain a valid certificate")
	}

	config := sarama.NewConfig()
	config.Version = sarama.V3_7_0_0
	config.ClientID = client.ClientID
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    rootCAs,
	}
	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	config.Net.SASL.User = client.Username
	config.Net.SASL.Password = client.Password
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 15 * time.Second
	config.Net.WriteTimeout = 15 * time.Second
	config.Metadata.Retry.Max = 10
	config.Metadata.Retry.Backoff = time.Second
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategySticky(),
	}

	return config, nil
}

func EnvOrDefault(key, fallback string) string {
	return envOrDefault(key, fallback)
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}
