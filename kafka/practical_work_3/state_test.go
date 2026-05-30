package main

import (
	"reflect"
	"testing"
)

func TestAddUniqueAndRemoveValues(t *testing.T) {
	values := addUnique([]string{"alice"}, []string{"bob", "alice", ""})
	if want := []string{"alice", "bob"}; !reflect.DeepEqual(values, want) {
		t.Fatalf("unexpected values after add: got %v, want %v", values, want)
	}

	values = removeValues(values, []string{"alice"})
	if want := []string{"bob"}; !reflect.DeepEqual(values, want) {
		t.Fatalf("unexpected values after remove: got %v, want %v", values, want)
	}
}
