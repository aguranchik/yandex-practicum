package main

import (
	"testing"

	"final_project/internal/model"
)

func TestBlockedEntry(t *testing.T) {
	entry := &model.BlacklistEntry{ProductID: "99999", Reason: "test"}
	got, blocked := blockedEntry(entry)
	if !blocked || got.ProductID != "99999" {
		t.Fatalf("blockedEntry() = %#v, %v", got, blocked)
	}
}

func TestBlockedEntryEmpty(t *testing.T) {
	if entry, blocked := blockedEntry(nil); blocked || entry != nil {
		t.Fatalf("blockedEntry(nil) = %#v, %v", entry, blocked)
	}
}
