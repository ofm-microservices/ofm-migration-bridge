package bridge

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestDeadLetterMessagePreservesSourceMetadata(t *testing.T) {
	message := kafka.Message{Topic: "cdc.auth.public.auth_credentials", Partition: 3, Offset: 17, Key: []byte("key"), Value: []byte("payload")}

	dlq := deadLetterMessage(message, 10, "schema validation failed")
	if string(dlq.Key) != "key" || string(dlq.Value) != "payload" {
		t.Fatalf("dead letter did not preserve key/value")
	}

	got := make(map[string]string, len(dlq.Headers))
	for _, header := range dlq.Headers {
		got[header.Key] = string(header.Value)
	}
	want := map[string]string{
		"original-topic":     "cdc.auth.public.auth_credentials",
		"original-partition": "3",
		"original-offset":    "17",
		"delivery-count":     "10",
		"dead-letter-reason": "schema validation failed",
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("header %q = %q, want %q", key, got[key], value)
		}
	}
}
