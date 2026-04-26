package main

import (
	"context"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

	func RunSingleMessageConsumer(ctx context.Context, cfg Config) error {
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			// bootstrap.servers задает список брокеров Kafka, к которым подключается consumer.
			"bootstrap.servers":  cfg.KafkaBrokers,
			// group.id объединяет экземпляры single consumer в одну consumer group.
			"group.id":           cfg.SingleGroupID,
			// client.id помогает отличать экземпляры consumer в логах и метриках брокера.
			"client.id":          fmt.Sprintf("%s-single-consumer", cfg.InstanceID),
			// session.timeout.ms задает интервал, после которого Kafka сочтет consumer недоступным без heartbeat.
			"session.timeout.ms": 6000,
		// enable.auto.commit=true оставляет коммит смещений на стороне клиента Kafka автоматически.
		"enable.auto.commit": true,
		// auto.offset.reset=earliest позволяет новой consumer group начать чтение с начала топика.
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		return fmt.Errorf("create single message consumer: %w", err)
	}
	defer func() {
		consumer.Close()
		log.Printf("single consumer: stopped")
	}()

	if err := consumer.SubscribeTopics([]string{cfg.KafkaTopic}, nil); err != nil {
		return fmt.Errorf("subscribe single message consumer: %w", err)
	}

	log.Printf("single consumer: subscribed to topic %q with group %q", cfg.KafkaTopic, cfg.SingleGroupID)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		event := consumer.Poll(cfg.PollTimeoutMs())
		if event == nil {
			continue
		}

		switch e := event.(type) {
		case *kafka.Message:
			message, err := UnmarshalMessage(e.Value)
			if err != nil {
				log.Printf("single consumer: deserialization error at %s[%d] offset=%v: %v", *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset, err)
				continue
			}

			log.Printf(
				"single consumer: received message from %s[%d] offset=%v: %+v",
				*e.TopicPartition.Topic,
				e.TopicPartition.Partition,
				e.TopicPartition.Offset,
				message,
			)
		case kafka.OffsetsCommitted:
		case kafka.Error:
			log.Printf("single consumer: kafka error: %v", e)
		default:
			log.Printf("single consumer: ignored event %T", e)
		}
	}
}
