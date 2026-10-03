package bridge

import (
	"errors"
	"strings"
	"testing"
)

func TestMapperMapsTransactionalOutboxRow(t *testing.T) {
	raw := []byte(`{"after":{"event_id":"11111111-1111-1111-1111-111111111111","aggregate_type":"orders","aggregate_id":"22222222-2222-2222-2222-222222222222","event_type":"order-service.orders.updated","operation":"u","schema_version":1,"payload":{"order_id":"22222222-2222-2222-2222-222222222222","status":"paid"},"occurred_at":"2026-08-03T10:00:00Z"},"source":{"service":"order-service","table":"outbox_events","ts_ms":1785751200000},"op":"c"}`)
	event, err := NewMapper().Map(raw)
	if err != nil {
		t.Fatalf("Map returned error: %v", err)
	}
	if event.EventID != "11111111-1111-1111-1111-111111111111" || event.EventType != "order-service.orders.updated" || event.Operation != "updated" {
		t.Fatalf("unexpected event identity: %#v", event)
	}
	if event.AggregateType != "orders" || event.AggregateID != "22222222-2222-2222-2222-222222222222" {
		t.Fatalf("unexpected aggregate: %#v", event)
	}
	if string(event.Payload) != `{"order_id":"22222222-2222-2222-2222-222222222222","status":"paid"}` {
		t.Fatalf("unexpected payload: %s", event.Payload)
	}
}

func TestMapperUnwrapsConnectorStringOutboxPayload(t *testing.T) {
	raw := []byte(`{"after":{"event_id":{"value":"33333333-3333-3333-3333-333333333333","set":true},"aggregate_type":{"value":"orders","set":true},"aggregate_id":{"value":"44444444-4444-4444-4444-444444444444","set":true},"event_type":{"value":"order-service.orders.changed","set":true},"schema_version":{"value":1,"set":true},"payload":{"value":"{\"order_id\":\"44444444-4444-4444-4444-444444444444\",\"status\":\"paid\"}","set":true},"occurred_at":{"value":"2026-08-03T10:00:00Z","set":true}},"source":{"db":"order_service","table":"outbox_events","ts_ms":1785751200000},"op":"c"}`)
	event, err := NewMapper().Map(raw)
	if err != nil {
		t.Fatalf("Map returned error: %v", err)
	}
	if string(event.Payload) != `{"order_id":"44444444-4444-4444-4444-444444444444","status":"paid"}` {
		t.Fatalf("unexpected decoded payload: %s", event.Payload)
	}
}

func TestMapperMapsAuthCredentials(t *testing.T) {
	raw := []byte(`{"after":{"user_id":"11111111-1111-1111-1111-111111111111","email":"alex@example.com","username":"alex"},"source":{"service":"auth-service","table":"auth_credentials","ts_ms":1700000000000},"op":"c"}`)
	event, err := NewMapper().Map(raw)
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != "auth.credentials.created" || event.AggregateType != "auth" || event.AggregateID == "" {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestMapperMapsUserDeleteFromBeforeImage(t *testing.T) {
	raw := []byte(`{"before":{"user_id":"11111111-1111-1111-1111-111111111111","username":"alex"},"after":null,"source":{"service":"user-service","table":"users","ts_ms":1700000000000},"op":"d"}`)
	event, err := NewMapper().Map(raw)
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != "user.profile.deactivated" || event.Operation != "deactivated" || event.AggregateID == "" {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestMapperIgnoresOutboxCleanupDelete(t *testing.T) {
	raw := []byte(`{"before":{"event_id":"11111111-1111-1111-1111-111111111111"},"after":null,"source":{"service":"chat-service","table":"outbox_events","ts_ms":1700000000000},"op":"d"}`)
	if _, err := NewMapper().Map(raw); !errors.Is(err, ErrIgnoredChange) {
		t.Fatalf("expected outbox cleanup delete to be ignored, got %v", err)
	}
}

func TestMapperRejectsMissingSource(t *testing.T) {
	if _, err := NewMapper().Map([]byte(`{"after":{"user_id":"x"},"op":"c"}`)); err == nil {
		t.Fatal("expected source validation error")
	}
}

func TestMapperDerivesServiceFromDebeziumDatabase(t *testing.T) {
	raw := []byte(`{"after":{"user_id":"11111111-1111-1111-1111-111111111111","username":"alex"},"source":{"db":"user_service","table":"users","ts_ms":1700000000000},"op":"u"}`)
	event, err := NewMapper().Map(raw)
	if err != nil {
		t.Fatal(err)
	}
	if event.SourceService != "user-service" || event.EventType != "user.profile.updated" {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestMapperNormalizesOrderChildTableToOrderAggregate(t *testing.T) {
	raw := []byte(`{"after":{"order_id":"11111111-1111-1111-1111-111111111111","id":"22222222-2222-2222-2222-222222222222"},"source":{"db":"order_service","table":"order_deliveries","ts_ms":1700000000000},"op":"u"}`)
	event, err := NewMapper().Map(raw)
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != "order-service.orders.changed" || event.AggregateType != "orders" {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestMapperUnwrapsPostgreSQLGRPCPayload(t *testing.T) {
	raw := []byte(`{"schema":{},"payload":{"before":null,"after":{"user_id":{"value":"44444444-4444-4444-4444-444444444444","set":true},"email":{"value":"cdc-live@example.test","set":true}},"source":{"name":"cdc.auth","db":"auth_service","table":"auth_credentials","ts_ms":1785611671574},"op":"c","ts_ms":1785611671584}}`)
	event, err := NewMapper().Map(raw)
	if err != nil {
		t.Fatal(err)
	}
	if event.AggregateID != "44444444-4444-4444-4444-444444444444" || event.EventType != "auth.credentials.created" {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestMapperUsesBoundedContextAggregateIDs(t *testing.T) {
	raw := []byte(`{"after":{"gig_id":{"value":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","set":true},"title":{"value":"fixture","set":true}},"source":{"db":"gig_service","table":"gigs","ts_ms":1700000000000},"op":"c"}`)
	event, err := NewMapper().Map(raw)
	if err != nil {
		t.Fatal(err)
	}
	if event.AggregateID != "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" || event.EventType != "gig-service.gigs.changed" {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestMapperMapsPaymentWebhookEvents(t *testing.T) {
	raw := []byte(`{"after":{"event_id":"abababab-abab-abab-abab-abababababab","provider":"stripe","order_id":"33333333-3333-3333-3333-333333333333","payment_intent_id":"22222222-2222-2222-2222-222222222222","status":"succeeded","payload_json":"{}"},"source":{"service":"payment-service","table":"payment_webhook_events","ts_ms":1700000000000},"op":"c"}`)
	event, err := NewMapper().Map(raw)
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != "payment-service.payment_webhook_events.changed" || event.AggregateType != "payment_webhook_events" || event.AggregateID != "22222222-2222-2222-2222-222222222222" || event.EventID == "abababab-abab-abab-abab-abababababab" {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestMapperMapsPaymentReleasesWithConnectorWrappers(t *testing.T) {
	raw := []byte(`{"after":{"payment_release_id":{"value":"f7f7f7f7-f7f7-f7f7-f7f7-f7f7f7f7f7f7","set":true},"status":{"value":"released","set":true}},"source":{"db":"payment_service","table":"payment_releases","ts_ms":1700000000000},"op":"u"}`)
	event, err := NewMapper().Map(raw)
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != "payment-service.payment_releases.changed" || event.AggregateType != "payment_releases" || event.AggregateID != "f7f7f7f7-f7f7-f7f7-f7f7-f7f7f7f7f7f7" {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestMapperMapsGigChildTables(t *testing.T) {
	for _, tc := range []struct{ table, key, id, eventType, aggregateType string }{
		{"gig_packages", "package_id", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "gig-service.gig_packages.changed", "gig_packages"},
		{"gig_questions", "question_id", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "gig-service.gig_questions.changed", "gig_questions"},
		{"gig_media", "media_id", "cccccccc-cccc-cccc-cccc-cccccccccccc", "gig-service.gig_media.changed", "gig_media"},
	} {
		raw := []byte(`{"after":{"` + tc.key + `":"` + tc.id + `","gig_id":"dddddddd-dddd-dddd-dddd-dddddddddddd"},"source":{"service":"gig-service","table":"` + tc.table + `","ts_ms":1700000000000},"op":"c"}`)
		event, err := NewMapper().Map(raw)
		if err != nil {
			t.Fatal(err)
		}
		if event.EventType != tc.eventType || event.AggregateType != tc.aggregateType || event.AggregateID != tc.id {
			t.Fatalf("unexpected %s event: %#v", tc.table, event)
		}
	}
}

func TestMapperMapsPostgreSQLCDCEnvelope(t *testing.T) {
	raw := []byte(`{"source":{"name":"migration-chat","db":"chat_service","table_name":"chats_by_order","ts_ms":1700000000000},"after":{"order_id":"chat-order-1","status":{"value":"open"}},"op":"c"}`)
	event, err := NewMapper().Map(raw)
	if err != nil {
		t.Fatal(err)
	}
	if event.SourceService != "chat-service" || event.EventType != "chat-service.chat.changed" || event.AggregateID != "chat-order-1" {
		t.Fatalf("unexpected PostgreSQL event: %#v", event)
	}
}

func TestMapperMapsPostgreSQLSagaAndRegistrationEnvelopes(t *testing.T) {
	for _, raw := range []string{
		`{"source":{"db":"order_saga","table_name":"order_saga_sessions","ts_ms":1700000000000},"after":{"saga_id":"saga-1","status":{"value":"started"}},"op":"c"}`,
		`{"source":{"db":"registration_saga_service","table_name":"registration_sessions","ts_ms":1700000000000},"after":{"session_id":"registration-1","status":{"value":"started"}},"op":"c"}`,
		`{"source":{"name":"migration-order-saga","table_name":"order_saga_steps","ts_ms":1700000000000},"after":{"saga_id":"saga-name-1","status":{"value":"started"}},"op":"c"}`,
		`{"source":{"name":"migration-registration-saga","table_name":"registration_steps","ts_ms":1700000000000},"after":{"session_id":"registration-name-1","status":{"value":"started"}},"op":"c"}`,
	} {
		if _, err := NewMapper().Map([]byte(raw)); err != nil {
			t.Fatalf("map PostgreSQL event: %v", err)
		}
	}
}

func TestMapperNormalizesPostgreSQLScalarAndTimestampFields(t *testing.T) {
	raw := []byte(`{"source":{"name":"migration-order-saga","db":"order_saga","table_name":"order_saga_sessions","ts_ms":1700000000000},"after":{"saga_id":"saga-1","order_id":"order-1","status":{"value":"started"},"created_at":{"value":1700000000000},"updated_at":{"value":1700000001000}},"op":"c"}`)
	event, err := NewMapper().Map(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(event.Payload); !strings.Contains(got, `"status":"started"`) || !strings.Contains(got, `"created_at":"2023-11-14T22:13:20Z"`) {
		t.Fatalf("connector wrappers were not normalized: %s", got)
	}
}

func TestMapperUsesRegistrationSecondaryIndexKeys(t *testing.T) {
	for _, tc := range []struct {
		table string
		key   string
		value string
	}{
		{table: "registration_sessions_by_email", key: "email", value: "index@example.com"},
		{table: "registration_sessions_by_username", key: "username", value: "index-user"},
	} {
		raw := []byte(`{"source":{"db":"registration_saga_service","table_name":"` + tc.table + `","ts_ms":1700000000000},"after":{"` + tc.key + `":"` + tc.value + `","status":{"value":"started"}},"op":"c"}`)
		event, err := NewMapper().Map(raw)
		if err != nil {
			t.Fatalf("map %s: %v", tc.table, err)
		}
		if event.AggregateID != tc.value || event.EventType != "registration-saga-service.registration.changed" {
			t.Fatalf("unexpected %s event: %#v", tc.table, event)
		}
	}
}

func TestMapperCoversEveryProductionWriteTable(t *testing.T) {
	tests := []struct {
		service string
		table   string
		db      string
	}{
		{"auth-service", "auth_credentials", "auth_service"},
		{"auth-service", "email_verification_codes", "auth_service"},
		{"auth-service", "auth_user_roles", "auth_service"},
		{"auth-service", "refresh_tokens", "auth_service"},
		{"user-service", "users", "user_service"},
		{"gig-service", "gigs", "gig_service"},
		{"gig-service", "gig_packages", "gig_service"},
		{"gig-service", "gig_questions", "gig_service"},
		{"gig-service", "gig_media", "gig_service"},
		{"order-service", "orders", "order_service"},
		{"order-service", "order_gig_snapshot", "order_service"},
		{"order-service", "order_question_snapshots", "order_service"},
		{"order-service", "order_requirement_answers", "order_service"},
		{"order-service", "order_buyer_messages", "order_service"},
		{"order-service", "order_attachments", "order_service"},
		{"order-service", "order_checkout_sessions", "order_service"},
		{"order-service", "order_deliveries", "order_service"},
		{"order-service", "order_revision_requests", "order_service"},
		{"order-service", "order_disputes", "order_service"},
		{"order-service", "order_delivery_files", "order_service"},
		{"payment-service", "payment_intents", "payment_service"},
		{"payment-service", "payment_webhook_events", "payment_service"},
		{"payment-service", "connect_accounts", "payment_service"},
		{"payment-service", "payment_releases", "payment_service"},
		{"review-service", "reviews", "review_service"},
		{"chat-service", "chats_by_order", "chat_service"},
		{"chat-service", "chat_messages_by_order", "chat_service"},
		{"chat-service", "chat_messages_by_id", "chat_service"},
		{"file-service", "files", "file_service"},
		{"order-saga-service", "order_saga_sessions", "order_saga"},
		{"order-saga-service", "order_saga_steps", "order_saga"},
		{"registration-saga-service", "registration_sessions", "registration_saga_service"},
		{"registration-saga-service", "registration_sessions_by_email", "registration_saga_service"},
		{"registration-saga-service", "registration_sessions_by_username", "registration_saga_service"},
		{"registration-saga-service", "registration_steps", "registration_saga_service"},
	}
	for _, tc := range tests {
		raw := []byte(`{"source":{"service":"` + tc.service + `","db":"` + tc.db + `","table_name":"` + tc.table + `","ts_ms":1700000000000},"after":{"id":"fixture-aggregate"},"op":"c"}`)
		event, err := NewMapper().Map(raw)
		if err != nil {
			t.Fatalf("map %s.%s: %v", tc.service, tc.table, err)
		}
		if event.SourceService != tc.service || event.AggregateID != "fixture-aggregate" || event.EventType == "" || event.SchemaVersion != 1 {
			t.Fatalf("incomplete mapping for %s.%s: %#v", tc.service, tc.table, event)
		}
	}
}
