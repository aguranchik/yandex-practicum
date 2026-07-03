package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"final_project/internal/kafkautil"
	"final_project/internal/model"
	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
)

const (
	filterGroup     goka.Group  = "product-filter"
	rawProducts     goka.Stream = "products.raw"
	filteredProduct goka.Stream = "products.filtered"
	blacklistTopic  goka.Stream = "blacklist.commands"
)

type jsonCodec struct {
	newValue func() any
}

func (c jsonCodec) Encode(value any) ([]byte, error) {
	return json.Marshal(value)
}

func (c jsonCodec) Decode(data []byte) (any, error) {
	value := c.newValue()
	if err := json.Unmarshal(data, value); err != nil {
		return nil, err
	}
	return value, nil
}

var (
	productCodec = jsonCodec{newValue: func() any { return new(model.Product) }}
	commandCodec = jsonCodec{newValue: func() any { return new(model.BlacklistCommand) }}
	entryCodec   = jsonCodec{newValue: func() any { return new(model.BlacklistEntry) }}
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC)
	client, err := kafkautil.FromEnv(
		"primary-kafka-1:9092,primary-kafka-2:9092,primary-kafka-3:9092",
		"processor",
		"processor-secret",
		"product-stream-processor",
	)
	if err != nil {
		log.Fatal(err)
	}
	saramaConfig, err := kafkautil.NewSaramaConfig(client)
	if err != nil {
		log.Fatal(err)
	}

	args := os.Args[1:]
	if len(args) == 0 || args[0] == "filter" {
		if err := runFilter(client.Brokers, saramaConfig); err != nil {
			log.Fatal(err)
		}
		return
	}
	if args[0] != "blacklist" {
		usage()
	}
	if err := runBlacklistCommand(client.Brokers, saramaConfig, args[1:]); err != nil {
		log.Fatal(err)
	}
}

func runFilter(brokers []string, config *sarama.Config) error {
	goka.ReplaceGlobalConfig(config)
	graph := goka.DefineGroup(
		filterGroup,
		goka.Input(rawProducts, productCodec, processProduct),
		goka.Input(blacklistTopic, commandCodec, processBlacklistCommand),
		goka.Output(filteredProduct, productCodec),
		goka.Persist(entryCodec),
	)
	processor, err := goka.NewProcessor(brokers, graph)
	if err != nil {
		return fmt.Errorf("create Goka processor: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("filter processor started: %s -> %s", rawProducts, filteredProduct)
	if err := processor.Run(ctx); err != nil && ctx.Err() == nil {
		return fmt.Errorf("run Goka processor: %w", err)
	}
	return nil
}

func processProduct(ctx goka.Context, message any) {
	product := message.(*model.Product)
	entry, blocked := blockedEntry(ctx.Value())
	if blocked {
		log.Printf("blocked product=%s reason=%q", product.ProductID, entry.Reason)
		return
	}
	ctx.Emit(filteredProduct, ctx.Key(), product)
	log.Printf("accepted product=%s", product.ProductID)
}

func processBlacklistCommand(ctx goka.Context, message any) {
	command := message.(*model.BlacklistCommand)
	switch command.Action {
	case "add":
		ctx.SetValue(&model.BlacklistEntry{
			ProductID: command.ProductID,
			Reason:    command.Reason,
			BlockedAt: command.UpdatedAt,
		})
		log.Printf("blacklist added product=%s", command.ProductID)
	case "remove":
		ctx.Delete()
		log.Printf("blacklist removed product=%s", command.ProductID)
	default:
		log.Printf("ignored blacklist action=%q product=%s", command.Action, command.ProductID)
	}
}

func blockedEntry(value any) (*model.BlacklistEntry, bool) {
	entry, ok := value.(*model.BlacklistEntry)
	return entry, ok && entry != nil
}

func runBlacklistCommand(brokers []string, config *sarama.Config, args []string) error {
	if len(args) == 0 {
		usage()
	}

	switch args[0] {
	case "add":
		if len(args) < 2 {
			usage()
		}
		reason := "blocked by operator"
		if len(args) > 2 {
			reason = strings.Join(args[2:], " ")
		}
		return publishCommand(brokers, config, model.BlacklistCommand{
			Action:    "add",
			ProductID: args[1],
			Reason:    reason,
			UpdatedAt: time.Now().UTC(),
		})
	case "remove":
		if len(args) != 2 {
			usage()
		}
		return publishCommand(brokers, config, model.BlacklistCommand{
			Action:    "remove",
			ProductID: args[1],
			UpdatedAt: time.Now().UTC(),
		})
	case "load":
		if len(args) != 2 {
			usage()
		}
		return loadBlacklist(brokers, config, args[1])
	case "list":
		return listBlacklist(brokers, config)
	default:
		usage()
	}
	return nil
}

func publishCommand(brokers []string, config *sarama.Config, command model.BlacklistCommand) error {
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return fmt.Errorf("create blacklist producer: %w", err)
	}
	defer producer.Close()
	payload, err := json.Marshal(command)
	if err != nil {
		return err
	}
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: string(blacklistTopic),
		Key:   sarama.StringEncoder(command.ProductID),
		Value: sarama.ByteEncoder(payload),
	})
	return err
}

func loadBlacklist(brokers []string, config *sarama.Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read blacklist file: %w", err)
	}
	var entries []struct {
		ProductID string `json:"product_id"`
		Reason    string `json:"reason"`
	}
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("decode blacklist file: %w", err)
	}
	for _, entry := range entries {
		if err := publishCommand(brokers, config, model.BlacklistCommand{
			Action:    "add",
			ProductID: entry.ProductID,
			Reason:    entry.Reason,
			UpdatedAt: time.Now().UTC(),
		}); err != nil {
			return err
		}
	}
	log.Printf("loaded %d blacklist entries", len(entries))
	return nil
}

func listBlacklist(brokers []string, config *sarama.Config) error {
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return fmt.Errorf("create blacklist consumer: %w", err)
	}
	defer consumer.Close()
	partitions, err := consumer.Partitions(string(blacklistTopic))
	if err != nil {
		return err
	}
	current := make(map[string]model.BlacklistCommand)
	for _, partition := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(string(blacklistTopic), partition, sarama.OffsetOldest)
		if err != nil {
			return err
		}
		highWatermark := partitionConsumer.HighWaterMarkOffset()
		for highWatermark > 0 {
			message := <-partitionConsumer.Messages()
			var command model.BlacklistCommand
			if err := json.Unmarshal(message.Value, &command); err != nil {
				partitionConsumer.Close()
				return err
			}
			current[command.ProductID] = command
			if message.Offset >= highWatermark-1 {
				break
			}
		}
		partitionConsumer.Close()
	}
	keys := make([]string, 0, len(current))
	for productID, command := range current {
		if command.Action == "add" {
			keys = append(keys, productID)
		}
	}
	sort.Strings(keys)
	for _, productID := range keys {
		fmt.Printf("%s\t%s\n", productID, current[productID].Reason)
	}
	return nil
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  stream-processor filter")
	fmt.Fprintln(os.Stderr, "  stream-processor blacklist add <product-id> [reason]")
	fmt.Fprintln(os.Stderr, "  stream-processor blacklist remove <product-id>")
	fmt.Fprintln(os.Stderr, "  stream-processor blacklist load <json-file>")
	fmt.Fprintln(os.Stderr, "  stream-processor blacklist list")
	os.Exit(2)
}
