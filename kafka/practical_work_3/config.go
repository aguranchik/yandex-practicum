package main

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	KafkaBrokers        []string
	MessagesTopic       string
	CensoredTopic       string
	FilteredTopic       string
	BlockedUsersTopic   string
	BannedWordsTopic    string
	BlockListGroup      string
	BannedWordsGroup    string
	CensorshipGroup     string
	BlockFilteringGroup string
	InstanceID          string
}

func LoadConfig() (Config, error) {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-instance"
	}

	brokers := splitCSV(readStringEnv("KAFKA_BROKERS", "kafka-0:9092,kafka-1:9092,kafka-2:9092"))
	if len(brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}

	return Config{
		KafkaBrokers:        brokers,
		MessagesTopic:       readStringEnv("MESSAGES_TOPIC", "messages"),
		CensoredTopic:       readStringEnv("CENSORED_TOPIC", "censored_messages"),
		FilteredTopic:       readStringEnv("FILTERED_TOPIC", "filtered_messages"),
		BlockedUsersTopic:   readStringEnv("BLOCKED_USERS_TOPIC", "blocked_users"),
		BannedWordsTopic:    readStringEnv("BANNED_WORDS_TOPIC", "banned_words"),
		BlockListGroup:      readStringEnv("BLOCK_LIST_GROUP", "pw3-block-list"),
		BannedWordsGroup:    readStringEnv("BANNED_WORDS_GROUP", "pw3-banned-words"),
		CensorshipGroup:     readStringEnv("CENSORSHIP_GROUP", "pw3-censorship"),
		BlockFilteringGroup: readStringEnv("BLOCK_FILTERING_GROUP", "pw3-block-filtering"),
		InstanceID:          readStringEnv("APP_INSTANCE_ID", hostname),
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
