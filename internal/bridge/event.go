package bridge

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/migration/events"
)

// ErrIgnoredChange marks a CDC change that is intentionally not published.
// Outbox status updates are acknowledgements, not new recovery commands.
var ErrIgnoredChange = errors.New("cdc change intentionally ignored")

// DebeziumEvent is the minimal Debezium envelope consumed by the bridge.
// The bridge intentionally keeps Debezium's storage payload private.
type DebeziumEvent struct {
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
	Key    json.RawMessage `json:"key"`
	Source struct {
		Service   string `json:"service"`
		Name      string `json:"name"`
		Database  string `json:"db"`
		Table     string `json:"table"`
		TableName string `json:"table_name"`
		Snapshot  string `json:"snapshot"`
		TsMs      int64  `json:"ts_ms"`
	} `json:"source"`
	Op      string          `json:"op"`
	TsMs    int64           `json:"ts_ms"`
	Payload json.RawMessage `json:"payload"`
}

// Mapper translates a Debezium change into a canonical migration event.
type Mapper interface {
	Map(input []byte) (events.Envelope, error)
}

type mapper struct{}

// NewMapper constructs the storage-to-event mapper.
func NewMapper() Mapper { return mapper{} }

func (mapper) Map(input []byte) (events.Envelope, error) {
	var change DebeziumEvent
	if err := json.Unmarshal(input, &change); err != nil {
		return events.Envelope{}, fmt.Errorf("decode debezium event: %w", err)
	}
	if len(change.Payload) > 0 && string(change.Payload) != "null" {
		var payload DebeziumEvent
		if err := json.Unmarshal(change.Payload, &payload); err != nil {
			return events.Envelope{}, fmt.Errorf("decode debezium payload: %w", err)
		}
		change.Before = payload.Before
		change.After = payload.After
		change.Key = payload.Key
		change.Source = payload.Source
		change.Source.Snapshot = payload.Source.Snapshot
		change.Op = payload.Op
		change.TsMs = payload.TsMs
	}
	if change.Source.Table == "" {
		change.Source.Table = change.Source.TableName
	}
	// Outbox rows are delivery records, not business aggregates. Services may
	// delete them after the bridge has read them; Debezium then emits a DELETE
	// record containing only the event_id (followed by a tombstone). There is
	// no business event to publish from that record, so acknowledge it instead
	// of retrying forever on an incomplete outbox identity.
	if change.Source.Table == "outbox_events" && strings.EqualFold(change.Op, "d") {
		return events.Envelope{}, ErrIgnoredChange
	}
	row := change.After
	if string(row) == "" || string(row) == "null" {
		row = change.Before
	}
	if strings.TrimSpace(string(row)) == "" || string(row) == "null" {
		return events.Envelope{}, fmt.Errorf("debezium event has no after row")
	}
	row = NormalizeConnectorRow(row)
	if change.Source.Table == "" {
		return events.Envelope{}, fmt.Errorf("debezium event has incomplete source metadata")
	}
	service := change.Source.Service
	if service == "" {
		service = serviceFromDatabase(change.Source.Database)
	}
	if service == "" {
		service = serviceFromConnectorName(change.Source.Name)
	}
	if service == "" {
		return events.Envelope{}, fmt.Errorf("debezium event has no source service or database")
	}
	if change.Source.TsMs == 0 {
		change.Source.TsMs = change.TsMs
	}
	if change.Source.Table == "outbox_events" || change.Source.Table == "migration_fallback_outbox" {
		// Debezium emits an initial snapshot after a connector restart. Existing
		// published fallback rows are historical acknowledgements, not new
		// recovery commands; replay only pending snapshot rows and preserve live
		// INSERT/UPDATE delivery for all later changes.
		if change.Source.Table == "migration_fallback_outbox" && strings.EqualFold(change.Source.Snapshot, "true") {
			var state struct {
				Status string `json:"status"`
			}
			if err := json.Unmarshal(row, &state); err == nil && strings.EqualFold(state.Status, "published") {
				return events.Envelope{}, ErrIgnoredChange
			}
		}
		if change.Source.Table == "migration_fallback_outbox" && change.Op != "c" && change.Op != "r" {
			// A connector restart can miss the INSERT and resume at a later
			// UPDATE. The row still carries the complete envelope; replay pending,
			// retrying, and published updates so an acknowledged CDC record can be
			// deliberately re-driven. Consumers use command_id idempotency to
			// suppress duplicates when the INSERT was already delivered.
			var state struct {
				Status string `json:"status"`
			}
			if err := json.Unmarshal(row, &state); err != nil ||
				(!strings.EqualFold(state.Status, "published") &&
					!strings.EqualFold(state.Status, "pending") &&
					!strings.EqualFold(state.Status, "retrying")) {
				return events.Envelope{}, fmt.Errorf("%w: table=%s op=%s", ErrIgnoredChange, change.Source.Table, change.Op)
			}
		}
		return mapOutboxEvent(row, service, change.Source.TsMs)
	}
	return events.Envelope{
		EventID:          deterministicEventID(change),
		EventType:        eventType(service, change.Source.Table, change.Op),
		Operation:        operation(change.Op),
		SchemaVersion:    1,
		AggregateType:    aggregateType(service, change.Source.Table),
		AggregateID:      aggregateIDForTable(change.Source.Table, row),
		AggregateVersion: aggregateVersion(row),
		SourceService:    service,
		OccurredAt:       time.UnixMilli(change.Source.TsMs).UTC(),
		Payload:          row,
	}, nil
}

type outboxRecord struct {
	EventID       string          `json:"event_id"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	EventType     string          `json:"event_type"`
	Operation     string          `json:"operation"`
	SchemaVersion int             `json:"schema_version"`
	Payload       json.RawMessage `json:"payload"`
	OccurredAt    time.Time       `json:"occurred_at"`
}

type fallbackOutboxRecord struct {
	CommandID string          `json:"command_id"`
	Envelope  json.RawMessage `json:"envelope"`
}

func mapOutboxEvent(row json.RawMessage, sourceService string, sourceTimestamp int64) (events.Envelope, error) {
	var fallback fallbackOutboxRecord
	if err := json.Unmarshal(row, &fallback); err == nil && fallback.CommandID != "" && len(fallback.Envelope) > 0 && string(fallback.Envelope) != "null" {
		var event events.Envelope
		envelope := fallback.Envelope
		var encodedEnvelope string
		if err := json.Unmarshal(envelope, &encodedEnvelope); err == nil {
			envelope = json.RawMessage(encodedEnvelope)
		}
		if err := json.Unmarshal(envelope, &event); err != nil {
			return events.Envelope{}, fmt.Errorf("decode fallback outbox envelope: %w", err)
		}
		if event.SourceService == "" {
			event.SourceService = sourceService
		}
		if event.SchemaVersion == 0 {
			event.SchemaVersion = 1
		}
		if event.EventID == "" || event.EventType == "" || len(event.Payload) == 0 {
			return events.Envelope{}, fmt.Errorf("fallback outbox envelope is incomplete")
		}
		// Older fallback captures were created before aggregate IDs were known
		// (for example, create-draft). The command ID is the durable identity of
		// that recovery operation and satisfies the canonical consumer contract.
		if event.AggregateID == "" {
			event.AggregateID = fallback.CommandID
		}
		return event, nil
	}
	var record outboxRecord
	if err := json.Unmarshal(row, &record); err != nil {
		return events.Envelope{}, fmt.Errorf("decode outbox row: %w", err)
	}
	if record.EventID == "" || record.AggregateType == "" || record.AggregateID == "" || record.EventType == "" {
		return events.Envelope{}, fmt.Errorf("outbox row is missing event identity")
	}
	if record.SchemaVersion == 0 {
		record.SchemaVersion = 1
	}
	if len(record.Payload) == 0 || string(record.Payload) == "null" {
		return events.Envelope{}, fmt.Errorf("outbox row has empty payload")
	}
	var encodedPayload string
	if err := json.Unmarshal(record.Payload, &encodedPayload); err == nil {
		record.Payload = json.RawMessage(encodedPayload)
	}
	if len(record.Payload) == 0 || string(record.Payload) == "null" {
		return events.Envelope{}, fmt.Errorf("outbox row has empty decoded payload")
	}
	if record.OccurredAt.IsZero() {
		record.OccurredAt = time.UnixMilli(sourceTimestamp).UTC()
	}
	return events.Envelope{
		EventID:       record.EventID,
		EventType:     record.EventType,
		Operation:     operation(record.Operation),
		SchemaVersion: record.SchemaVersion,
		AggregateType: record.AggregateType,
		AggregateID:   record.AggregateID,
		SourceService: sourceService,
		OccurredAt:    record.OccurredAt,
		Payload:       record.Payload,
	}, nil
}

// NormalizeConnectorRow removes PostgreSQL Debezium connector value wrappers so the
// canonical event carries the owning service's row shape rather than connector
// implementation details.
func NormalizeConnectorRow(row json.RawMessage) json.RawMessage {
	var value any
	if json.Unmarshal(row, &value) != nil {
		return row
	}
	value = normalizeConnectorValue(value)
	normalized, err := json.Marshal(value)
	if err != nil {
		return row
	}
	return normalized
}

func normalizeConnectorValue(value any) any {
	switch typed := value.(type) {
	case []any:
		for index := range typed {
			typed[index] = normalizeConnectorValue(typed[index])
		}
	case map[string]any:
		if wrapped, ok := typed["value"]; ok {
			// PostgreSQL's connector emits {value,set}; PostgreSQL Debezium CDC emits
			// scalar fields as {value}. Unwrap both forms before the
			// canonical event reaches a projection decoder.
			if _, present := typed["set"]; present || len(typed) == 1 {
				return normalizeConnectorValue(wrapped)
			}
		}
		for key, nested := range typed {
			normalized := normalizeConnectorValue(nested)
			if isTimestampField(key) {
				normalized = normalizeTimestamp(normalized)
			}
			typed[key] = normalized
		}
	}
	return value
}

func isTimestampField(key string) bool {
	switch key {
	case "created_at", "updated_at", "published_at", "occurred_at", "next_attempt_at", "locked_until", "expires_at":
		return true
	default:
		return false
	}
}

func normalizeTimestamp(value any) any {
	number, ok := value.(float64)
	if !ok || number == 0 {
		return value
	}
	return time.UnixMilli(int64(number)).UTC().Format(time.RFC3339Nano)
}

func serviceFromDatabase(database string) string {
	switch database {
	case "auth_service":
		return "auth-service"
	case "user_service":
		return "user-service"
	case "gig_service":
		return "gig-service"
	case "order_service":
		return "order-service"
	case "payment_service":
		return "payment-service"
	case "review_service":
		return "review-service"
	case "chat_service":
		return "chat-service"
	case "order_saga_service":
		return "order-saga-service"
	case "order_saga":
		return "order-saga-service"
	case "registration_saga_service":
		return "registration-saga-service"
	case "file_service":
		return "file-service"
	default:
		return ""
	}
}

// serviceFromConnectorName maps PostgreSQL Debezium CDC connector names when the connector
// omits Debezium's database field. PostgreSQL runtime uses names such as
// migration-order-saga and migration-registration-saga.
func serviceFromConnectorName(name string) string {
	switch {
	case strings.Contains(name, "monolith"):
		return "monolith-fallback"
	case strings.Contains(name, "order-saga"):
		return "order-saga-service"
	case strings.Contains(name, "registration-saga"):
		return "registration-saga-service"
	case strings.Contains(name, "chat"):
		return "chat-service"
	case strings.Contains(name, "file"):
		return "file-service"
	default:
		return ""
	}
}

func deterministicEventID(change DebeziumEvent) string {
	data, _ := json.Marshal(change)
	// A stable hash is used until the source connector supplies a transaction
	// position. It gives Kafka and processed_events the same dedup key.
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", hash[0:4], hash[4:6], hash[6:8], hash[8:10], hash[10:16])
}

func eventType(service, table, op string) string {
	if service == "auth-service" && strings.Contains(table, "credential") {
		return "auth.credentials." + operation(op)
	}
	if service == "user-service" && strings.Contains(table, "user") {
		return "user.profile." + operation(op)
	}
	return service + "." + aggregateName(service, table) + ".changed"
}

func operation(op string) string {
	switch op {
	case "c":
		return "created"
	case "d":
		return "deactivated"
	case "created", "updated", "deactivated":
		return op
	default:
		return "updated"
	}
}

func aggregateType(service, table string) string {
	if service == "user-service" && strings.Contains(table, "user") {
		return "user"
	}
	if service == "auth-service" && (strings.Contains(table, "credential") || strings.Contains(table, "token") || strings.Contains(table, "verification")) {
		return "auth"
	}
	return aggregateName(service, table)
}

func aggregateName(service, table string) string {
	switch service {
	case "gig-service":
		if strings.Contains(table, "package") {
			return "gig_packages"
		}
		if strings.Contains(table, "question") {
			return "gig_questions"
		}
		if strings.Contains(table, "media") {
			return "gig_media"
		}
		return "gigs"
	case "order-service":
		return "orders"
	case "payment-service":
		if strings.Contains(table, "webhook_event") {
			return "payment_webhook_events"
		}
		if strings.Contains(table, "release") {
			return "payment_releases"
		}
		if strings.Contains(table, "connect_account") {
			return "connect_accounts"
		}
		return "payment_intents"
	case "review-service":
		return "reviews"
	case "chat-service":
		return "chat"
	case "order-saga-service":
		return "order_saga"
	case "registration-saga-service":
		return "registration"
	case "file-service":
		return "files"
	default:
		return table
	}
}

func aggregateID(after json.RawMessage) string {
	var row map[string]any
	_ = json.Unmarshal(after, &row)
	for _, key := range []string{"payment_intent_id", "payment_release_id", "user_id", "credential_id", "package_id", "question_id", "media_id", "gig_id", "order_id", "review_id", "message_id", "session_id", "saga_id", "file_id", "event_id", "email", "username", "id"} {
		if value, ok := row[key].(string); ok {
			return value
		}
	}
	return ""
}

// aggregateIDForTable prevents a child-table CDC row from falling back to its
// parent aggregate. A gig_packages row without package_id must not create a
// package mapping for gig_id.
func aggregateIDForTable(table string, after json.RawMessage) string {
	if table == "gig_packages" {
		var row map[string]any
		_ = json.Unmarshal(after, &row)
		for _, key := range []string{"package_id", "id"} {
			if value, ok := row[key].(string); ok && value != "" {
				return value
			}
		}
		return ""
	}
	return aggregateID(after)
}

func aggregateVersion(payload json.RawMessage) int64 {
	var row map[string]any
	if json.Unmarshal(payload, &row) != nil {
		return 0
	}
	for _, key := range []string{"aggregate_version", "version", "updated_version"} {
		if value, ok := row[key].(float64); ok && value > 0 {
			return int64(value)
		}
	}
	return 0
}
