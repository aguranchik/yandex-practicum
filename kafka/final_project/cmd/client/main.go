package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"final_project/internal/kafkautil"
	"final_project/internal/model"
	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	requestType := os.Args[1]
	flags := flag.NewFlagSet(requestType, flag.ExitOnError)
	userID := flags.String("user", "demo-user", "client user ID")
	if err := flags.Parse(os.Args[2:]); err != nil {
		log.Fatal(err)
	}

	var query string
	switch requestType {
	case "search":
		query = strings.TrimSpace(strings.Join(flags.Args(), " "))
		if query == "" {
			usage()
		}
	case "recommendations":
		if len(flags.Args()) != 0 {
			usage()
		}
	default:
		usage()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	databaseURL := kafkautil.EnvOrDefault("DATABASE_URL", "postgres://marketplace:marketplace-secret@postgres:5432/marketplace")
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	event := model.ClientEvent{
		RequestID:   newRequestID(),
		UserID:      *userID,
		RequestType: requestType,
		Query:       query,
		RequestedAt: time.Now().UTC(),
	}
	if err := publishEvent(event); err != nil {
		log.Fatalf("publish client event: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO client_requests (request_id, user_id, request_type, query, requested_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (request_id) DO NOTHING`,
		event.RequestID, event.UserID, event.RequestType, event.Query, event.RequestedAt,
	); err != nil {
		log.Fatalf("store client request: %v", err)
	}

	if requestType == "search" {
		if err := searchProducts(ctx, pool, query); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := printRecommendations(ctx, pool, *userID); err != nil {
		log.Fatal(err)
	}
}

func publishEvent(event model.ClientEvent) error {
	client, err := kafkautil.FromEnv(
		"primary-kafka-1:9092,primary-kafka-2:9092,primary-kafka-3:9092",
		"client",
		"client-secret",
		"client-cli",
	)
	if err != nil {
		return err
	}
	config, err := kafkautil.NewSaramaConfig(client)
	if err != nil {
		return err
	}
	producer, err := sarama.NewSyncProducer(client.Brokers, config)
	if err != nil {
		return err
	}
	defer producer.Close()
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "client.events",
		Key:   sarama.StringEncoder(event.UserID),
		Value: sarama.ByteEncoder(payload),
	})
	return err
}

func searchProducts(ctx context.Context, pool *pgxpool.Pool, query string) error {
	rows, err := pool.Query(ctx, `
		SELECT data
		FROM products
		WHERE name ILIKE '%' || $1 || '%'
		ORDER BY name
		LIMIT 20`, query)
	if err != nil {
		return fmt.Errorf("search products: %w", err)
	}
	defer rows.Close()

	results := make([]json.RawMessage, 0)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		results = append(results, json.RawMessage(raw))
	}
	return writeJSON(results)
}

func printRecommendations(ctx context.Context, pool *pgxpool.Pool, userID string) error {
	rows, err := pool.Query(ctx, `
		SELECT user_id, product_id, product_name, reason, generated_at
		FROM recommendations
		WHERE user_id = $1
		ORDER BY generated_at DESC`, userID)
	if err != nil {
		return fmt.Errorf("load recommendations: %w", err)
	}
	defer rows.Close()

	results := make([]model.Recommendation, 0)
	for rows.Next() {
		var recommendation model.Recommendation
		if err := rows.Scan(
			&recommendation.UserID,
			&recommendation.ProductID,
			&recommendation.ProductName,
			&recommendation.Reason,
			&recommendation.GeneratedAt,
		); err != nil {
			return err
		}
		results = append(results, recommendation)
	}
	return writeJSON(results)
}

func writeJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

func newRequestID() string {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("request-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buffer)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  client search --user <user-id> <product name>")
	fmt.Fprintln(os.Stderr, "  client recommendations --user <user-id>")
	os.Exit(2)
}
