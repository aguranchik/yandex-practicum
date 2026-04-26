package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	KafkaBrokers     string
	KafkaTopic       string
	SingleGroupID    string
	BatchGroupID     string
	ProducerInterval time.Duration
	PollTimeout      time.Duration
	BatchSize        int
	FetchMinBytes    int
	FetchMaxWaitMs   int
	ProducerAcks     string
	ProducerRetries  int
	InstanceID       string
}

func LoadConfig() (Config, error) {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-instance"
	}

	producerInterval, err := readDurationEnv("PRODUCER_INTERVAL", time.Second)
	if err != nil {
		return Config{}, err
	}

	pollTimeout, err := readDurationEnv("POLL_TIMEOUT", time.Second)
	if err != nil {
		return Config{}, err
	}

	batchSize, err := readIntEnv("BATCH_SIZE", 10)
	if err != nil {
		return Config{}, err
	}
	if batchSize <= 0 {
		return Config{}, fmt.Errorf("BATCH_SIZE must be greater than zero")
	}

	fetchMinBytes, err := readIntEnv("FETCH_MIN_BYTES", 1024)
	if err != nil {
		return Config{}, err
	}

	fetchMaxWaitMs, err := readIntEnv("FETCH_MAX_WAIT_MS", 5000)
	if err != nil {
		return Config{}, err
	}

	producerRetries, err := readIntEnv("PRODUCER_RETRIES", 10)
	if err != nil {
		return Config{}, err
	}

	return Config{
		KafkaBrokers:     readStringEnv("KAFKA_BROKERS", "kafka-0:9092,kafka-1:9092,kafka-2:9092"),
		KafkaTopic:       readStringEnv("KAFKA_TOPIC", "practical-work-2-orders"),
		SingleGroupID:    readStringEnv("SINGLE_GROUP_ID", "pw2-single-group"),
		BatchGroupID:     readStringEnv("BATCH_GROUP_ID", "pw2-batch-group"),
		ProducerInterval: producerInterval,
		PollTimeout:      pollTimeout,
		BatchSize:        batchSize,
		FetchMinBytes:    fetchMinBytes,
		FetchMaxWaitMs:   fetchMaxWaitMs,
		ProducerAcks:     readStringEnv("PRODUCER_ACKS", "all"),
		ProducerRetries:  producerRetries,
		InstanceID:       readStringEnv("APP_INSTANCE_ID", hostname),
	}, nil
}

func (c Config) PollTimeoutMs() int {
	timeoutMs := int(c.PollTimeout.Milliseconds())
	if timeoutMs <= 0 {
		return 1000
	}

	return timeoutMs
}

func readStringEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

func readIntEnv(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}

	return parsed, nil
}

func readDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", key, err)
	}

	return parsed, nil
}
