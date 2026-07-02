package appconfig

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultTopic         = "pw7-events"
	defaultConsumerGroup = "pw7-consumer"
)

type Config struct {
	Brokers           []string
	Topic             string
	ConsumerGroup     string
	KafkaUsername     string
	KafkaPassword     string
	KafkaCAFile       string
	SchemaRegistryURL string
	RegistryUsername  string
	RegistryPassword  string
	RegistryCAFile    string
	SchemaFile        string
	MessageCount      int
	IdleTimeout       time.Duration
}

func Load() (Config, error) {
	brokers := splitCSV(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must contain at least one host:port")
	}

	kafkaUsername := strings.TrimSpace(os.Getenv("KAFKA_USERNAME"))
	kafkaPassword := os.Getenv("KAFKA_PASSWORD")
	if kafkaUsername == "" || kafkaPassword == "" {
		return Config{}, fmt.Errorf("KAFKA_USERNAME and KAFKA_PASSWORD are required")
	}

	kafkaCAFile := strings.TrimSpace(os.Getenv("KAFKA_CA_FILE"))
	if kafkaCAFile == "" {
		return Config{}, fmt.Errorf("KAFKA_CA_FILE is required")
	}

	registryURL := strings.TrimRight(strings.TrimSpace(os.Getenv("SCHEMA_REGISTRY_URL")), "/")
	if registryURL == "" {
		return Config{}, fmt.Errorf("SCHEMA_REGISTRY_URL is required")
	}

	messageCount, err := intEnv("MESSAGE_COUNT", 5)
	if err != nil {
		return Config{}, err
	}
	if messageCount < 0 {
		return Config{}, fmt.Errorf("MESSAGE_COUNT must be non-negative")
	}

	idleTimeout, err := durationEnv("CONSUMER_IDLE_TIMEOUT", 20*time.Second)
	if err != nil {
		return Config{}, err
	}

	registryUsername := env("SCHEMA_REGISTRY_USERNAME", kafkaUsername)
	registryPassword := env("SCHEMA_REGISTRY_PASSWORD", kafkaPassword)
	registryCAFile := env("SCHEMA_REGISTRY_CA_FILE", kafkaCAFile)

	return Config{
		Brokers:           brokers,
		Topic:             env("KAFKA_TOPIC", defaultTopic),
		ConsumerGroup:     env("KAFKA_GROUP_ID", defaultConsumerGroup),
		KafkaUsername:     kafkaUsername,
		KafkaPassword:     kafkaPassword,
		KafkaCAFile:       kafkaCAFile,
		SchemaRegistryURL: registryURL,
		RegistryUsername:  registryUsername,
		RegistryPassword:  registryPassword,
		RegistryCAFile:    registryCAFile,
		SchemaFile:        env("AVRO_SCHEMA_FILE", "schemas/pw7-event.avsc"),
		MessageCount:      messageCount,
		IdleTimeout:       idleTimeout,
	}, nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func intEnv(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return value, nil
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return value, nil
}
