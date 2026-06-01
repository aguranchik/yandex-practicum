package main

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	Brokers []string
	Topics  []string
	GroupID string
}

func LoadConfig() (Config, error) {
	brokers := splitCSV(readStringEnv("KAFKA_BROKERS", "localhost:19094,localhost:19095,localhost:19096"))
	if len(brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}

	topics := splitCSV(readStringEnv("KAFKA_TOPICS", "postgres.public.users,postgres.public.orders"))
	if len(topics) == 0 {
		return Config{}, fmt.Errorf("KAFKA_TOPICS must contain at least one topic")
	}

	return Config{
		Brokers: brokers,
		Topics:  topics,
		GroupID: readStringEnv("KAFKA_GROUP_ID", "pw5-cdc-console-consumer"),
	}, nil
}

func readStringEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}

	return result
}
