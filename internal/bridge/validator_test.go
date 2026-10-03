package bridge

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/migration/events"
)

func validationEvent() events.Envelope {
	return events.Envelope{
		EventID:       "11111111-1111-1111-1111-111111111111",
		EventType:     "order-service.orders.changed",
		SchemaVersion: 1,
		AggregateType: "orders",
		AggregateID:   "22222222-2222-2222-2222-222222222222",
		SourceService: "order-service",
		OccurredAt:    time.Unix(1, 0).UTC(),
		Payload:       []byte(`{"order_id":"22222222-2222-2222-2222-222222222222"}`),
	}
}

func TestRegistryValidatorCreatesMissingArtifact(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/groups/default/artifacts/order-service.orders.changed.v1" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/groups/default/artifacts" {
			t.Fatalf("unexpected registry request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("unexpected content type: %s", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := NewRegistryValidator(server.URL).Validate(t.Context(), validationEvent()); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestRegistryValidatorRejectsSchemaFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))
	defer server.Close()

	if err := NewRegistryValidator(server.URL).Validate(t.Context(), validationEvent()); err == nil {
		t.Fatal("expected schema validation error")
	}
}
