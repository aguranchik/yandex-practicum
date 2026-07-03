package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"final_project/internal/kafkautil"
	"final_project/internal/model"
	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5/pgxpool"
)

type indexHandler struct {
	pool *pgxpool.Pool
}

func (h *indexHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *indexHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *indexHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var err error
		switch message.Topic {
		case "products.filtered":
			err = h.storeProduct(session.Context(), message.Value)
		case "recommendations":
			err = h.storeRecommendation(session.Context(), message.Value)
		default:
			err = fmt.Errorf("unsupported topic %s", message.Topic)
		}
		if err != nil {
			return err
		}
		session.MarkMessage(message, "stored")
	}
	return nil
}

func (h *indexHandler) storeProduct(ctx context.Context, payload []byte) error {
	var product model.Product
	if err := json.Unmarshal(payload, &product); err != nil {
		return fmt.Errorf("decode product: %w", err)
	}
	raw, err := json.Marshal(product)
	if err != nil {
		return err
	}
	_, err = h.pool.Exec(ctx, `
		INSERT INTO products (
			product_id, name, description, price_amount, currency, category,
			brand, available, reserved, sku, store_id, data, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (product_id) DO UPDATE SET
			name = EXCLUDED.name,
			description = EXCLUDED.description,
			price_amount = EXCLUDED.price_amount,
			currency = EXCLUDED.currency,
			category = EXCLUDED.category,
			brand = EXCLUDED.brand,
			available = EXCLUDED.available,
			reserved = EXCLUDED.reserved,
			sku = EXCLUDED.sku,
			store_id = EXCLUDED.store_id,
			data = EXCLUDED.data,
			updated_at = EXCLUDED.updated_at`,
		product.ProductID, product.Name, product.Description, product.Price.Amount,
		product.Price.Currency, product.Category, product.Brand, product.Stock.Available,
		product.Stock.Reserved, product.SKU, product.StoreID, raw, product.UpdatedAt,
	)
	if err == nil {
		log.Printf("stored product=%s", product.ProductID)
	}
	return err
}

func (h *indexHandler) storeRecommendation(ctx context.Context, payload []byte) error {
	var recommendation model.Recommendation
	if err := json.Unmarshal(payload, &recommendation); err != nil {
		return fmt.Errorf("decode recommendation: %w", err)
	}
	_, err := h.pool.Exec(ctx, `
		INSERT INTO recommendations (user_id, product_id, product_name, reason, generated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (user_id, product_id) DO UPDATE SET
			product_name = EXCLUDED.product_name,
			reason = EXCLUDED.reason,
			generated_at = EXCLUDED.generated_at`,
		recommendation.UserID, recommendation.ProductID, recommendation.ProductName,
		recommendation.Reason, recommendation.GeneratedAt,
	)
	if err == nil {
		log.Printf("stored recommendation user=%s product=%s", recommendation.UserID, recommendation.ProductID)
	}
	return err
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	databaseURL := kafkautil.EnvOrDefault("DATABASE_URL", "postgres://marketplace:marketplace-secret@postgres:5432/marketplace")
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("connect to PostgreSQL: %v", err)
	}
	defer pool.Close()
	if err := waitForDatabase(ctx, pool); err != nil {
		log.Fatal(err)
	}

	handler := &indexHandler{pool: pool}
	errCh := make(chan error, 2)
	go consume(ctx, "PRIMARY_", "storage-products", []string{"products.filtered"}, handler, errCh)
	go consume(ctx, "SECONDARY_", "storage-recommendations", []string{"recommendations"}, handler, errCh)

	select {
	case <-ctx.Done():
		log.Printf("storage indexer stopped")
	case err := <-errCh:
		log.Fatal(err)
	}
}

func consume(ctx context.Context, prefix, groupID string, topics []string, handler sarama.ConsumerGroupHandler, errCh chan<- error) {
	defaultBrokers := "primary-kafka-1:9092,primary-kafka-2:9092,primary-kafka-3:9092"
	if prefix == "SECONDARY_" {
		defaultBrokers = "secondary-kafka-1:9092,secondary-kafka-2:9092,secondary-kafka-3:9092"
	}
	client, err := kafkautil.FromPrefixedEnv(prefix, defaultBrokers, "storage", "storage-secret", groupID)
	if err != nil {
		errCh <- err
		return
	}
	config, err := kafkautil.NewSaramaConfig(client)
	if err != nil {
		errCh <- err
		return
	}
	group, err := sarama.NewConsumerGroup(client.Brokers, groupID, config)
	if err != nil {
		errCh <- err
		return
	}
	defer group.Close()
	for ctx.Err() == nil {
		if err := group.Consume(ctx, topics, handler); err != nil {
			errCh <- fmt.Errorf("consume %v: %w", topics, err)
			return
		}
	}
}

func waitForDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	for attempt := 1; attempt <= 30; attempt++ {
		if err := pool.Ping(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("PostgreSQL did not become ready")
}
