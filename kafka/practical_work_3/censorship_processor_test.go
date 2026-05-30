package main

import "testing"

func TestMaskBannedWords(t *testing.T) {
	got := maskBannedWords("Spam is bad, but spamming is a different word", []string{"spam", "bad"})
	want := "**** is ***, but spamming is a different word"

	if got != want {
		t.Fatalf("unexpected masked text: got %q, want %q", got, want)
	}
}
