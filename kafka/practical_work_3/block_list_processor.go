package main

import (
	"context"
	"fmt"
	"log"

	"github.com/lovoo/goka"
)

func RunBlockListProcessor(ctx context.Context, cfg Config) error {
	group := goka.Group(cfg.BlockListGroup)
	blockedUsersTopic := goka.Stream(cfg.BlockedUsersTopic)

	graph := goka.DefineGroup(
		group,
		goka.Input(blockedUsersTopic, new(JSONCodec[BlockEvent]), func(ctx goka.Context, value interface{}) {
			event := value.(BlockEvent)
			userID := normalizeUserID(event.UserID)
			blockedUserID := normalizeUserID(event.BlockedUserID)
			if userID == "" || blockedUserID == "" {
				log.Printf("block-list: skipped invalid event: %+v", event)
				return
			}

			current := BlockList{}
			if value := ctx.Value(); value != nil {
				current = value.(BlockList)
			}

			switch event.Action {
			case BlockActionBlock:
				current.BlockedUserIDs = addUnique(current.BlockedUserIDs, []string{blockedUserID})
			case BlockActionUnblock:
				current.BlockedUserIDs = removeValues(current.BlockedUserIDs, []string{blockedUserID})
			default:
				log.Printf("block-list: skipped event with unsupported action %q: %+v", event.Action, event)
				return
			}

			if event.UpdatedAt == "" {
				event.UpdatedAt = utcNow()
			}
			current.UpdatedAt = event.UpdatedAt

			ctx.SetValue(current)
			log.Printf("block-list: user %q has blocked users: %v", userID, current.BlockedUserIDs)
		}),
		goka.Persist(new(JSONCodec[BlockList])),
	)

	processor, err := goka.NewProcessor(cfg.KafkaBrokers, graph)
	if err != nil {
		return fmt.Errorf("create block-list processor: %w", err)
	}

	log.Printf("block-list: started with group %q", cfg.BlockListGroup)
	if err := processor.Run(ctx); err != nil {
		return fmt.Errorf("run block-list processor: %w", err)
	}

	log.Printf("block-list: stopped")
	return nil
}
