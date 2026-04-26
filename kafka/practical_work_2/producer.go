package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

	func RunProducer(ctx context.Context, cfg Config) error {
		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			// bootstrap.servers задает список брокеров Kafka, к которым подключается продюсер.
			"bootstrap.servers": cfg.KafkaBrokers,
			// client.id помогает отличать экземпляры продюсера в логах и метриках брокера.
			"client.id":         fmt.Sprintf("%s-producer", cfg.InstanceID),
			// acks=all заставляет брокер дождаться подтверждения от всех in-sync replicas.
			"acks":              cfg.ProducerAcks,
			// retries позволяет повторить отправку при временном сбое и поддерживает гарантию At Least Once.
			"retries":           cfg.ProducerRetries,
		})
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}

	log.Printf("producer: started for topic %q with instance %q", cfg.KafkaTopic, cfg.InstanceID)

	var deliveryWG sync.WaitGroup
	deliveryWG.Add(1)

	go func() {
		defer deliveryWG.Done()

		for event := range producer.Events() {
			switch e := event.(type) {
			case *kafka.Message:
				messageID, _ := e.Opaque.(string)
				if e.TopicPartition.Error != nil {
					log.Printf("producer: delivery error for message %q: %v", messageID, e.TopicPartition.Error)
					continue
				}

				log.Printf(
					"producer: message %q delivered to %s[%d] offset=%v",
					messageID,
					*e.TopicPartition.Topic,
					e.TopicPartition.Partition,
					e.TopicPartition.Offset,
				)
			case kafka.Error:
				log.Printf("producer: client error: %v", e)
			default:
				log.Printf("producer: ignored event %T", e)
			}
		}
	}()

	defer func() {
		remaining := producer.Flush(5000)
		if remaining > 0 {
			log.Printf("producer: %d messages remained in local queue after flush", remaining)
		}

		producer.Close()
		deliveryWG.Wait()
		log.Printf("producer: stopped")
	}()

	sequence := 0
	ticker := newStoppableTicker(cfg.ProducerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			sequence++

			message := NewTestMessage(cfg.InstanceID, sequence)
			payload, err := message.Marshal()
			if err != nil {
				log.Printf("producer: serialization error for message %q: %v", message.MessageID, err)
				continue
			}

			log.Printf("producer: sending message: %+v", message)

			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &cfg.KafkaTopic,
					Partition: kafka.PartitionAny,
				},
				Key:    []byte(message.CustomerID),
				Value:  payload,
				Opaque: message.MessageID,
			}, nil)
			if err != nil {
				log.Printf("producer: enqueue error for message %q: %v", message.MessageID, err)
			}
		}
	}
}
