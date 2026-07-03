package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"final_project/internal/kafkautil"
	"final_project/internal/model"
	"github.com/IBM/sarama"
)

func main() {
	filePath := flag.String("file", kafkautil.EnvOrDefault("PRODUCTS_FILE", "/data/products.jsonl"), "path to a JSON Lines product file")
	topic := flag.String("topic", kafkautil.EnvOrDefault("PRODUCTS_TOPIC", "products.raw"), "target Kafka topic")
	flag.Parse()

	client, err := kafkautil.FromEnv(
		"primary-kafka-1:9092,primary-kafka-2:9092,primary-kafka-3:9092",
		"shop",
		"shop-secret",
		"shop-producer",
	)
	if err != nil {
		log.Fatal(err)
	}
	saramaConfig, err := kafkautil.NewSaramaConfig(client)
	if err != nil {
		log.Fatal(err)
	}
	producer, err := sarama.NewSyncProducer(client.Brokers, saramaConfig)
	if err != nil {
		log.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	count, err := publishFile(producer, *topic, *filePath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("published %d products to %s", count, *topic)
}

func publishFile(producer sarama.SyncProducer, topic, filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("open products file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	count := 0
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		var product model.Product
		if err := json.Unmarshal(scanner.Bytes(), &product); err != nil {
			return count, fmt.Errorf("decode product on line %d: %w", lineNumber, err)
		}
		if err := product.Validate(); err != nil {
			return count, fmt.Errorf("validate product on line %d: %w", lineNumber, err)
		}
		payload, err := json.Marshal(product)
		if err != nil {
			return count, fmt.Errorf("encode product %s: %w", product.ProductID, err)
		}
		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(product.ProductID),
			Value: sarama.ByteEncoder(payload),
		})
		if err != nil {
			return count, fmt.Errorf("publish product %s: %w", product.ProductID, err)
		}
		log.Printf("product=%s partition=%d offset=%d", product.ProductID, partition, offset)
		count++
	}
	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("read products file: %w", err)
	}
	return count, nil
}
