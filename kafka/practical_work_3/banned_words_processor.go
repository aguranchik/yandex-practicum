package main

import (
	"context"
	"fmt"
	"log"

	"github.com/lovoo/goka"
)

func RunBannedWordsProcessor(ctx context.Context, cfg Config) error {
	group := goka.Group(cfg.BannedWordsGroup)
	bannedWordsTopic := goka.Stream(cfg.BannedWordsTopic)

	graph := goka.DefineGroup(
		group,
		goka.Input(bannedWordsTopic, new(JSONCodec[BannedWordsUpdate]), func(ctx goka.Context, value interface{}) {
			update := value.(BannedWordsUpdate)
			current := BannedWordsState{}
			if value := ctx.Value(); value != nil {
				current = value.(BannedWordsState)
			}

			words := make([]string, 0, len(update.Words))
			for _, word := range update.Words {
				if normalized := normalizeWord(word); normalized != "" {
					words = append(words, normalized)
				}
			}

			switch update.Action {
			case BannedWordsActionReplace:
				current.Words = addUnique(nil, words)
			case BannedWordsActionAdd:
				current.Words = addUnique(current.Words, words)
			case BannedWordsActionRemove:
				current.Words = removeValues(current.Words, words)
			default:
				log.Printf("banned-words: skipped update with unsupported action %q: %+v", update.Action, update)
				return
			}

			if update.UpdatedAt == "" {
				update.UpdatedAt = utcNow()
			}
			current.UpdatedAt = update.UpdatedAt

			ctx.SetValue(current)
			log.Printf("banned-words: active words: %v", current.Words)
		}),
		goka.Persist(new(JSONCodec[BannedWordsState])),
	)

	processor, err := goka.NewProcessor(cfg.KafkaBrokers, graph)
	if err != nil {
		return fmt.Errorf("create banned-words processor: %w", err)
	}

	log.Printf("banned-words: started with group %q", cfg.BannedWordsGroup)
	if err := processor.Run(ctx); err != nil {
		return fmt.Errorf("run banned-words processor: %w", err)
	}

	log.Printf("banned-words: stopped")
	return nil
}
