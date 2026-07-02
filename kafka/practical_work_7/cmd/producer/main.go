package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"practical_work_7/internal/appconfig"
	"practical_work_7/internal/avrocodec"
	"practical_work_7/internal/kafkaclient"
	"practical_work_7/internal/registry"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	cfg, err := appconfig.Load()
	if err != nil {
		return err
	}
	schema, err := os.ReadFile(cfg.SchemaFile)
	if err != nil {
		return fmt.Errorf("read Avro schema %q: %w", cfg.SchemaFile, err)
	}

	registryClient, err := registry.New(
		cfg.SchemaRegistryURL,
		cfg.RegistryUsername,
		cfg.RegistryPassword,
		cfg.RegistryCAFile,
	)
	if err != nil {
		return err
	}
	subject := cfg.Topic + "-value"
	schemaID, err := registryClient.Register(ctx, subject, string(schema))
	if err != nil {
		return fmt.Errorf("register subject %q: %w", subject, err)
	}
	log.Printf("schema registered: subject=%s id=%d", subject, schemaID)

	producer, err := kafkaclient.NewProducer(kafkaclient.Config{
		Brokers:  cfg.Brokers,
		Username: cfg.KafkaUsername,
		Password: cfg.KafkaPassword,
		CAFile:   cfg.KafkaCAFile,
	})
	if err != nil {
		return fmt.Errorf("create Kafka producer: %w", err)
	}
	defer producer.Close()

	for index := 1; index <= cfg.MessageCount; index++ {
		eventID := fmt.Sprintf("event-%03d", index)
		record := map[string]any{
			"event_id":   eventID,
			"source":     "go-producer",
			"message":    fmt.Sprintf("Practical work 7 message #%d", index),
			"created_at": time.Now().UTC().UnixMilli(),
		}
		payload, err := avrocodec.Encode(string(schema), schemaID, record)
		if err != nil {
			return fmt.Errorf("encode %s: %w", eventID, err)
		}

		result := producer.ProduceSync(ctx, &kgo.Record{
			Topic: cfg.Topic,
			Key:   []byte(eventID),
			Value: payload,
		})
		if err := result.FirstErr(); err != nil {
			return fmt.Errorf("send %s: %w", eventID, err)
		}
		recordMetadata := result[0].Record
		log.Printf(
			"message delivered: event_id=%s topic=%s partition=%d offset=%d",
			eventID,
			recordMetadata.Topic,
			recordMetadata.Partition,
			recordMetadata.Offset,
		)
	}

	return nil
}
