package main

import (
	"context"
	"fmt"
	"log"

	"github.com/lovoo/goka"
)

func RunBlockFilterProcessor(ctx context.Context, cfg Config) error {
	group := goka.Group(cfg.BlockFilteringGroup)
	censoredTopic := goka.Stream(cfg.CensoredTopic)
	filteredTopic := goka.Stream(cfg.FilteredTopic)
	blockListTable := goka.GroupTable(goka.Group(cfg.BlockListGroup))

	graph := goka.DefineGroup(
		group,
		goka.Input(censoredTopic, new(JSONCodec[ChatMessage]), func(ctx goka.Context, value interface{}) {
			message := value.(ChatMessage)

			blockList := BlockList{}
			if value := ctx.Join(blockListTable); value != nil {
				blockList = value.(BlockList)
			}

			if containsString(blockList.BlockedUserIDs, message.SenderID) {
				log.Printf(
					"block-filter: dropped message %q because recipient %q blocked sender %q",
					message.MessageID,
					message.RecipientID,
					message.SenderID,
				)
				return
			}

			ctx.Emit(filteredTopic, message.RecipientID, message)
			log.Printf("block-filter: delivered message %q to filtered_messages", message.MessageID)
		}),
		goka.Join(blockListTable, new(JSONCodec[BlockList])),
		goka.Output(filteredTopic, new(JSONCodec[ChatMessage])),
	)

	processor, err := goka.NewProcessor(cfg.KafkaBrokers, graph)
	if err != nil {
		return fmt.Errorf("create block-filter processor: %w", err)
	}

	log.Printf("block-filter: started with group %q", cfg.BlockFilteringGroup)
	if err := processor.Run(ctx); err != nil {
		return fmt.Errorf("run block-filter processor: %w", err)
	}

	log.Printf("block-filter: stopped")
	return nil
}
