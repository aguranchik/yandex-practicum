package avrocodec

import "testing"

const testSchema = `{
  "type": "record",
  "name": "Event",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "created_at", "type": "long"}
  ]
}`

func TestEncodeDecode(t *testing.T) {
	want := map[string]any{"event_id": "event-1", "created_at": int64(42)}
	payload, err := Encode(testSchema, 17, want)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	id, err := SchemaID(payload)
	if err != nil {
		t.Fatalf("SchemaID() error = %v", err)
	}
	if id != 17 {
		t.Fatalf("SchemaID() = %d, want 17", id)
	}

	got, err := Decode(testSchema, payload)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got["event_id"] != want["event_id"] || got["created_at"] != want["created_at"] {
		t.Fatalf("Decode() = %#v, want %#v", got, want)
	}
}

func TestSchemaIDRejectsInvalidFraming(t *testing.T) {
	if _, err := SchemaID([]byte{1, 0, 0, 0, 1}); err == nil {
		t.Fatal("SchemaID() error = nil, want an invalid magic byte error")
	}
}
