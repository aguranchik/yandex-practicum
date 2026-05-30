package main

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/lovoo/goka"
)

func RunCensorshipProcessor(ctx context.Context, cfg Config) error {
	group := goka.Group(cfg.CensorshipGroup)
	messagesTopic := goka.Stream(cfg.MessagesTopic)
	censoredTopic := goka.Stream(cfg.CensoredTopic)
	bannedWordsTable := goka.GroupTable(goka.Group(cfg.BannedWordsGroup))

	graph := goka.DefineGroup(
		group,
		goka.Input(messagesTopic, new(JSONCodec[ChatMessage]), func(ctx goka.Context, value interface{}) {
			message := value.(ChatMessage)
			if message.RecipientID == "" || message.SenderID == "" {
				log.Printf("censorship: skipped invalid message: %+v", message)
				return
			}

			state := BannedWordsState{}
			if value := ctx.Lookup(bannedWordsTable, BannedWordsKey); value != nil {
				state = value.(BannedWordsState)
			}

			originalText := message.Text
			message.Text = maskBannedWords(message.Text, state.Words)

			ctx.Emit(censoredTopic, message.RecipientID, message)
			log.Printf(
				"censorship: message %q from %q to %q censored=%t",
				message.MessageID,
				message.SenderID,
				message.RecipientID,
				originalText != message.Text,
			)
		}),
		goka.Lookup(bannedWordsTable, new(JSONCodec[BannedWordsState])),
		goka.Output(censoredTopic, new(JSONCodec[ChatMessage])),
	)

	processor, err := goka.NewProcessor(cfg.KafkaBrokers, graph)
	if err != nil {
		return fmt.Errorf("create censorship processor: %w", err)
	}

	log.Printf("censorship: started with group %q", cfg.CensorshipGroup)
	if err := processor.Run(ctx); err != nil {
		return fmt.Errorf("run censorship processor: %w", err)
	}

	log.Printf("censorship: stopped")
	return nil
}

func maskBannedWords(text string, bannedWords []string) string {
	result := text
	for _, word := range bannedWords {
		word = normalizeWord(word)
		if word == "" {
			continue
		}

		pattern := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(word) + `\b`)
		result = pattern.ReplaceAllStringFunc(result, func(match string) string {
			return strings.Repeat("*", len([]rune(match)))
		})
	}

	return result
}
