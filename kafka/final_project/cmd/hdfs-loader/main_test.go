package main

import "testing"

func TestDirectoryForTopic(t *testing.T) {
	tests := map[string]string{
		"primary.products.filtered": "/marketplace/products",
		"primary.client.events":     "/marketplace/client-events",
		"other":                     "/marketplace/unknown",
	}
	for topic, want := range tests {
		if got := directoryForTopic(topic); got != want {
			t.Errorf("directoryForTopic(%q) = %q, want %q", topic, got, want)
		}
	}
}
