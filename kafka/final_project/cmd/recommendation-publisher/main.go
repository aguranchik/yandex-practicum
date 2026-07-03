package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path"
	"strings"

	hdfsclient "final_project/internal/hdfs"
	"final_project/internal/kafkautil"
	"final_project/internal/model"
	"github.com/IBM/sarama"
)

func main() {
	ctx := context.Background()
	hdfs, err := hdfsclient.NewClient(
		kafkautil.EnvOrDefault("WEBHDFS_URL", "http://hadoop-namenode:9870"),
		kafkautil.EnvOrDefault("HDFS_USER", "root"),
	)
	if err != nil {
		log.Fatal(err)
	}
	client, err := kafkautil.FromEnv(
		"secondary-kafka-1:9092,secondary-kafka-2:9092,secondary-kafka-3:9092",
		"analytics",
		"analytics-secret",
		"recommendation-publisher",
	)
	if err != nil {
		log.Fatal(err)
	}
	config, err := kafkautil.NewSaramaConfig(client)
	if err != nil {
		log.Fatal(err)
	}
	producer, err := sarama.NewSyncProducer(client.Brokers, config)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	count, err := publishRecommendations(ctx, hdfs, producer, "/marketplace/recommendations")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("published %d recommendations", count)
}

func publishRecommendations(ctx context.Context, hdfs *hdfsclient.Client, producer sarama.SyncProducer, directory string) (int, error) {
	statuses, err := hdfs.ListStatus(ctx, directory)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, status := range statuses {
		if status.Type != "FILE" || !strings.HasPrefix(status.PathSuffix, "part-") {
			continue
		}
		data, err := hdfs.Open(ctx, path.Join(directory, status.PathSuffix))
		if err != nil {
			return count, err
		}
		scanner := bufio.NewScanner(bytes.NewReader(data))
		for scanner.Scan() {
			var recommendation model.Recommendation
			if err := json.Unmarshal(scanner.Bytes(), &recommendation); err != nil {
				return count, fmt.Errorf("decode recommendation: %w", err)
			}
			key := recommendation.UserID + ":" + recommendation.ProductID
			if _, _, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: "recommendations",
				Key:   sarama.StringEncoder(key),
				Value: sarama.ByteEncoder(append([]byte(nil), scanner.Bytes()...)),
			}); err != nil {
				return count, err
			}
			count++
		}
		if err := scanner.Err(); err != nil {
			return count, err
		}
	}
	return count, nil
}
