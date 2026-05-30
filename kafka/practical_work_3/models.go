package main

import "time"

const (
	BannedWordsKey = "global"

	BlockActionBlock   = "block"
	BlockActionUnblock = "unblock"

	BannedWordsActionReplace = "replace"
	BannedWordsActionAdd     = "add"
	BannedWordsActionRemove  = "remove"
)

type ChatMessage struct {
	MessageID   string `json:"message_id"`
	SenderID    string `json:"sender_id"`
	RecipientID string `json:"recipient_id"`
	Text        string `json:"text"`
	CreatedAt   string `json:"created_at"`
}

type BlockEvent struct {
	UserID        string `json:"user_id"`
	BlockedUserID string `json:"blocked_user_id"`
	Action        string `json:"action"`
	UpdatedAt     string `json:"updated_at"`
}

type BannedWordsUpdate struct {
	Words     []string `json:"words"`
	Action    string   `json:"action"`
	UpdatedAt string   `json:"updated_at"`
}

type BlockList struct {
	BlockedUserIDs []string `json:"blocked_user_ids"`
	UpdatedAt      string   `json:"updated_at"`
}

type BannedWordsState struct {
	Words     []string `json:"words"`
	UpdatedAt string   `json:"updated_at"`
}

func utcNow() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
