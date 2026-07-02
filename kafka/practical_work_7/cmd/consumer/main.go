package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"practical_work_7/internal/appconfig"
	"practical_work_7/internal/avrocodec"
	"practical_work_7/internal/kafkaclient"
	"practical_work_7/internal/registry"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	cfg, err := appconfig.Load()
	if err != nil {
		return err
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
	consumer, err := kafkaclient.NewConsumer(kafkaclient.Config{
		Brokers:       cfg.Brokers,
		Username:      cfg.KafkaUsername,
		Password:      cfg.KafkaPassword,
		CAFile:        cfg.KafkaCAFile,
		ConsumerGroup: cfg.ConsumerGroup,
		Topic:         cfg.Topic,
	})
	if err != nil {
		return fmt.Errorf("create Kafka consumer: %w", err)
	}
	defer consumer.Close()

	log.Printf("consumer started: topic=%s group=%s", cfg.Topic, cfg.ConsumerGroup)
	schemas := make(map[int]string)
	read := 0
	idleTimer := time.NewTimer(cfg.IdleTimeout)
	defer idleTimer.Stop()

	for cfg.MessageCount == 0 || read < cfg.MessageCount {
		pollCtx, cancel := context.WithTimeout(ctx, time.Second)
		fetches := consumer.PollFetches(pollCtx)
		cancel()

		if ctx.Err() != nil {
			return nil
		}
		if fetches.IsClientClosed() {
			return nil
		}
		for _, fetchError := range fetches.Errors() {
			if errors.Is(fetchError.Err, context.DeadlineExceeded) || errors.Is(fetchError.Err, context.Canceled) {
				continue
			}
			return fmt.Errorf("fetch %s[%d]: %w", fetchError.Topic, fetchError.Partition, fetchError.Err)
		}

		processed := false
		fetches.EachRecord(func(record *kgo.Record) {
			if cfg.MessageCount > 0 && read >= cfg.MessageCount {
				return
			}
			if err != nil {
				return
			}
			var schemaID int
			schemaID, err = avrocodec.SchemaID(record.Value)
			if err != nil {
				err = fmt.Errorf("read schema id at %s[%d] offset %d: %w", record.Topic, record.Partition, record.Offset, err)
				return
			}
			schema, found := schemas[schemaID]
			if !found {
				schema, err = registryClient.SchemaByID(ctx, schemaID)
				if err != nil {
					err = fmt.Errorf("load schema id %d: %w", schemaID, err)
					return
				}
				schemas[schemaID] = schema
			}
			var decoded map[string]any
			decoded, err = avrocodec.Decode(schema, record.Value)
			if err != nil {
				err = fmt.Errorf("decode %s[%d] offset %d: %w", record.Topic, record.Partition, record.Offset, err)
				return
			}
			log.Printf(
				"message received: topic=%s partition=%d offset=%d schema_id=%d value=%s",
				record.Topic,
				record.Partition,
				record.Offset,
				schemaID,
				avrocodec.JSON(decoded),
			)
			if commitErr := consumer.CommitRecords(ctx, record); commitErr != nil {
				err = fmt.Errorf("commit offset %d: %w", record.Offset, commitErr)
				return
			}
			read++
			processed = true
		})
		if err != nil {
			return err
		}

		if processed {
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(cfg.IdleTimeout)
			continue
		}

		select {
		case <-idleTimer.C:
			log.Printf("no messages received for %s; exiting after %d messages", cfg.IdleTimeout, read)
			return nil
		default:
		}
	}

	log.Printf("consumer finished: messages=%d", read)
	return nil
}
