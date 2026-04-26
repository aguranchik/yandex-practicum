package main

import (
	"encoding/json"
	"fmt"
	"time"
)

type Message struct {
	MessageID        string  `json:"message_id"`
	OrderID          string  `json:"order_id"`
	CustomerID       string  `json:"customer_id"`
	ProducerInstance string  `json:"producer_instance"`
	CreatedAt        string  `json:"created_at"`
	Status           string  `json:"status"`
	Amount           float64 `json:"amount"`
}

func NewTestMessage(instanceID string, sequence int) Message {
	statuses := []string{"created", "validated", "ready-for-delivery"}

	return Message{
		MessageID:        fmt.Sprintf("%s-msg-%06d", instanceID, sequence),
		OrderID:          fmt.Sprintf("order-%06d", sequence),
		CustomerID:       fmt.Sprintf("customer-%02d", (sequence%5)+1),
		ProducerInstance: instanceID,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		Status:           statuses[sequence%len(statuses)],
		Amount:           100 + float64(sequence%9)*25.5,
	}
}

func (m Message) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func UnmarshalMessage(payload []byte) (Message, error) {
	var message Message

	if err := json.Unmarshal(payload, &message); err != nil {
		return Message{}, err
	}

	return message, nil
}
