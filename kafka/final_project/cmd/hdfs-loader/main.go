package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	hdfsclient "final_project/internal/hdfs"
	"final_project/internal/kafkautil"
	"github.com/IBM/sarama"
)

type loaderHandler struct {
	hdfs *hdfsclient.Client
}

func (h *loaderHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *loaderHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *loaderHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		directory := directoryForTopic(message.Topic)
		if err := h.hdfs.Mkdirs(session.Context(), directory); err != nil {
			return err
		}
		filename := fmt.Sprintf(
			"event-%d-%d-%d.jsonl",
			time.Now().UTC().UnixNano(),
			message.Partition,
			message.Offset,
		)
		payload := append(append([]byte(nil), message.Value...), '\n')
		if err := h.hdfs.Create(session.Context(), path.Join(directory, filename), payload); err != nil {
			return err
		}
		log.Printf("stored topic=%s partition=%d offset=%d in HDFS", message.Topic, message.Partition, message.Offset)
		session.MarkMessage(message, "stored-in-hdfs")
	}
	return nil
}

func directoryForTopic(topic string) string {
	switch {
	case strings.HasSuffix(topic, "products.filtered"):
		return "/marketplace/products"
	case strings.HasSuffix(topic, "client.events"):
		return "/marketplace/client-events"
	default:
		return "/marketplace/unknown"
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	hdfsURL := kafkautil.EnvOrDefault("WEBHDFS_URL", "http://hadoop-namenode:9870")
	hdfs, err := hdfsclient.NewClient(hdfsURL, kafkautil.EnvOrDefault("HDFS_USER", "root"))
	if err != nil {
		log.Fatal(err)
	}
	client, err := kafkautil.FromEnv(
		"secondary-kafka-1:9092,secondary-kafka-2:9092,secondary-kafka-3:9092",
		"analytics",
		"analytics-secret",
		"hdfs-loader",
	)
	if err != nil {
		log.Fatal(err)
	}
	config, err := kafkautil.NewSaramaConfig(client)
	if err != nil {
		log.Fatal(err)
	}
	group, err := sarama.NewConsumerGroup(client.Brokers, "hdfs-loader", config)
	if err != nil {
		log.Fatal(err)
	}
	defer group.Close()
	handler := &loaderHandler{hdfs: hdfs}
	topics := []string{"primary.products.filtered", "primary.client.events"}
	for ctx.Err() == nil {
		if err := group.Consume(ctx, topics, handler); err != nil {
			log.Fatalf("consume mirrored topics: %v", err)
		}
	}
}
