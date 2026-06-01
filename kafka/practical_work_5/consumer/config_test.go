package main

import (
	"reflect"
	"testing"
)

func TestSplitCSV(t *testing.T) {
	got := splitCSV(" kafka-0:9092, ,kafka-1:9092 ")
	want := []string{"kafka-0:9092", "kafka-1:9092"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected csv split: got %v, want %v", got, want)
	}
}
