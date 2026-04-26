package main

import (
	"context"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

	func RunBatchMessageConsumer(ctx context.Context, cfg Config) error {
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			// bootstrap.servers задает список брокеров Kafka, к которым подключается consumer.
			"bootstrap.servers":  cfg.KafkaBrokers,
			// group.id объединяет экземпляры batch consumer в одну consumer group.
			"group.id":           cfg.BatchGroupID,
			// client.id помогает отличать экземпляры consumer в логах и метриках брокера.
			"client.id":          fmt.Sprintf("%s-batch-consumer", cfg.InstanceID),
			// session.timeout.ms задает интервал, после которого Kafka сочтет consumer недоступным без heartbeat.
			"session.timeout.ms": 6000,
			// enable.auto.commit=false отключает автоматический commit, чтобы подтверждать offsets вручную после пачки.
			"enable.auto.commit": false,
			// auto.offset.reset=earliest позволяет новой consumer group начать чтение с начала топика.
			"auto.offset.reset":  "earliest",
		// fetch.min.bytes просит брокер подождать, пока не накопится хотя бы такой объем данных.
		"fetch.min.bytes": cfg.FetchMinBytes,
		// В librdkafka этот параметр называется fetch.wait.max.ms и задает верхнюю границу ожидания ответа от брокера.
		"fetch.wait.max.ms": cfg.FetchMaxWaitMs,
	})
	if err != nil {
		return fmt.Errorf("create batch message consumer: %w", err)
	}
	defer func() {
		consumer.Close()
		log.Printf("batch consumer: stopped")
	}()

	if err := consumer.SubscribeTopics([]string{cfg.KafkaTopic}, nil); err != nil {
		return fmt.Errorf("subscribe batch message consumer: %w", err)
	}

	log.Printf("batch consumer: subscribed to topic %q with group %q", cfg.KafkaTopic, cfg.BatchGroupID)

	batch := make([]*kafka.Message, 0, cfg.BatchSize)

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				log.Printf("batch consumer: processing %d buffered messages before shutdown", len(batch))
				processBatch(consumer, batch)
			}

			return nil
		default:
		}

		event := consumer.Poll(cfg.PollTimeoutMs())
		if event == nil {
			continue
		}

		switch e := event.(type) {
		case *kafka.Message:
			batch = append(batch, e)
			if len(batch) < cfg.BatchSize {
				continue
			}

			processBatch(consumer, batch)
			batch = batch[:0]
		case kafka.Error:
			log.Printf("batch consumer: kafka error: %v", e)
		default:
			log.Printf("batch consumer: ignored event %T", e)
		}
	}
}

func processBatch(consumer *kafka.Consumer, batch []*kafka.Message) {
	log.Printf("batch consumer: processing batch of %d messages", len(batch))

	hasDeserializationError := false

	for _, kafkaMessage := range batch {
		message, err := UnmarshalMessage(kafkaMessage.Value)
		if err != nil {
			hasDeserializationError = true
			log.Printf(
				"batch consumer: deserialization error at %s[%d] offset=%v: %v",
				*kafkaMessage.TopicPartition.Topic,
				kafkaMessage.TopicPartition.Partition,
				kafkaMessage.TopicPartition.Offset,
				err,
			)
			continue
		}

		log.Printf(
			"batch consumer: received message from %s[%d] offset=%v: %+v",
			*kafkaMessage.TopicPartition.Topic,
			kafkaMessage.TopicPartition.Partition,
			kafkaMessage.TopicPartition.Offset,
			message,
		)
	}

	if hasDeserializationError {
		log.Printf("batch consumer: skipped commit for batch of %d messages because at least one message failed deserialization", len(batch))
		return
	}

	if _, err := consumer.Commit(); err != nil {
		log.Printf("batch consumer: commit error after batch of %d messages: %v", len(batch), err)
		return
	}

	log.Printf("batch consumer: committed offsets after batch of %d messages", len(batch))
}
