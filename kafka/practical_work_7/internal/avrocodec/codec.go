package avrocodec

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/linkedin/goavro/v2"
)

const framingSize = 5

func Encode(schema string, schemaID int, value map[string]any) ([]byte, error) {
	if schemaID <= 0 {
		return nil, fmt.Errorf("schema id must be positive")
	}
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("create Avro codec: %w", err)
	}
	binaryPayload, err := codec.BinaryFromNative(nil, value)
	if err != nil {
		return nil, fmt.Errorf("encode Avro record: %w", err)
	}

	payload := make([]byte, framingSize, framingSize+len(binaryPayload))
	payload[0] = 0
	binary.BigEndian.PutUint32(payload[1:framingSize], uint32(schemaID))
	payload = append(payload, binaryPayload...)
	return payload, nil
}

func SchemaID(payload []byte) (int, error) {
	if len(payload) < framingSize {
		return 0, fmt.Errorf("payload is too short: got %d bytes", len(payload))
	}
	if payload[0] != 0 {
		return 0, fmt.Errorf("unsupported Confluent framing magic byte %d", payload[0])
	}
	return int(binary.BigEndian.Uint32(payload[1:framingSize])), nil
}

func Decode(schema string, payload []byte) (map[string]any, error) {
	if _, err := SchemaID(payload); err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("create Avro codec: %w", err)
	}
	value, remaining, err := codec.NativeFromBinary(payload[framingSize:])
	if err != nil {
		return nil, fmt.Errorf("decode Avro record: %w", err)
	}
	if len(remaining) != 0 {
		return nil, fmt.Errorf("Avro payload has %d trailing bytes", len(remaining))
	}
	record, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("decoded Avro value has unexpected type %T", value)
	}
	return record, nil
}

func JSON(record map[string]any) string {
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Sprintf("%v", record)
	}
	return string(payload)
}
