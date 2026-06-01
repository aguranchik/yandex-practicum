package main

import (
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
)

type CDCConsumerGroupHandler struct{}

func (CDCConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Printf("consumer: session started")
	return nil
}

func (CDCConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Printf("consumer: session stopped")
	return nil
}

func (CDCConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		printMessage(message)
		session.MarkMessage(message, "")
	}

	return nil
}

func printMessage(message *sarama.ConsumerMessage) {
	var payload any
	if err := json.Unmarshal(message.Value, &payload); err != nil {
		log.Printf(
			"consumer: topic=%s partition=%d offset=%d key=%s raw=%s json_error=%v",
			message.Topic,
			message.Partition,
			message.Offset,
			string(message.Key),
			string(message.Value),
			err,
		)
		return
	}

	prettyPayload, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		prettyPayload = message.Value
	}

	log.Printf(
		"consumer: topic=%s partition=%d offset=%d key=%s value=\n%s",
		message.Topic,
		message.Partition,
		message.Offset,
		string(message.Key),
		string(prettyPayload),
	)
}
