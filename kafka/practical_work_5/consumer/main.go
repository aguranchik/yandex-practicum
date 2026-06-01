package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC)

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = "practical-work-5-cdc-consumer"
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Return.Errors = true

	consumerGroup, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, saramaConfig)
	if err != nil {
		log.Fatalf("create consumer group: %v", err)
	}
	defer func() {
		if err := consumerGroup.Close(); err != nil {
			log.Printf("close consumer group: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		for err := range consumerGroup.Errors() {
			log.Printf("consumer group error: %v", err)
		}
	}()

	log.Printf("consumer: started group=%q topics=%v brokers=%v", cfg.GroupID, cfg.Topics, cfg.Brokers)

	handler := CDCConsumerGroupHandler{}
	for ctx.Err() == nil {
		if err := consumerGroup.Consume(ctx, cfg.Topics, handler); err != nil {
			log.Printf("consume: %v", err)
		}
	}

	log.Printf("consumer: shutdown completed")
}
