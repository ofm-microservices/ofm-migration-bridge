package bridge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/migration/events"
	transportkafka "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"github.com/segmentio/kafka-go"
)

// Bridge consumes Debezium records from Kafka and emits validated canonical events.
type Bridge struct {
	brokers, topics, group string
	mapper                 Mapper
	validator              Validator
	publisher              Publisher
	processed              atomic.Uint64
	reconnects             atomic.Uint64
	publishErrors          atomic.Uint64
}

// ServeMetrics exposes bridge health and counters for Prometheus.
func (b *Bridge) ServeMetrics(address string) error {
	return http.ListenAndServe(address, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, "migration_bridge_up 1\nmigration_bridge_processed_events_total %d\nmigration_bridge_reconnects_total %d\nmigration_bridge_publish_errors_total %d\n", b.processed.Load(), b.reconnects.Load(), b.publishErrors.Load())
	}))
}

// New constructs a Kafka CDC bridge.
func New(brokers, topics, group string, mapper Mapper, validator Validator, publisher Publisher) (*Bridge, error) {
	if brokers == "" || topics == "" || group == "" || mapper == nil || validator == nil || publisher == nil {
		return nil, fmt.Errorf("Kafka bridge configuration or dependencies are incomplete")
	}
	return &Bridge{brokers: brokers, topics: topics, group: group, mapper: mapper, validator: validator, publisher: publisher}, nil
}

// Run consumes source topics until the context is cancelled. Failed records are
// left uncommitted so Kafka can redeliver them; invalid records are sent to DLQ.
func (b *Bridge) Run(ctx context.Context) error {
	for _, topic := range strings.Split(b.topics, ",") {
		// Recovery CDC must replay an initial Debezium snapshot after a connector
		// rebuild. kafka-go uses the committed group offset when one exists, so
		// FirstOffset only affects a genuinely new group and makes restart
		// recovery durable without a separate polling reconciler.
		go b.consumeWithReconnect(ctx, strings.TrimSpace(topic))
	}
	<-ctx.Done()
	return nil
}

// consumeWithReconnect keeps a recovery stream alive across broker restarts.
// A transient Kafka connection failure must not permanently disable CDC until
// the container itself is manually recreated.
func (b *Bridge) consumeWithReconnect(ctx context.Context, topic string) {
	backoff := time.Second
	for ctx.Err() == nil {
		// A kafka.Reader cannot reliably be reused after its broker connection
		// or group session has failed. Build a fresh reader so the consumer
		// rejoins the group and resumes from Kafka's committed offset.
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{b.brokers},
			Topic:   topic,
			// Each reader subscribes to exactly one topic. Isolate its
			// consumer-group membership so Kafka cannot assign that topic's
			// partition to one of the bridge's readers for another topic.
			GroupID:        b.group + "-" + strings.NewReplacer(".", "-", ",", "-", " ", "-").Replace(topic),
			StartOffset:    kafka.FirstOffset,
			MinBytes:       1,
			MaxBytes:       10 << 20,
			MaxWait:        50 * time.Millisecond,
			CommitInterval: 500 * time.Millisecond,
		})
		log.Printf("migration bridge reader starting topic=%s group=%s brokers=%s", topic, b.group, b.brokers)
		b.consume(ctx, reader)
		_ = reader.Close()
		if ctx.Err() != nil {
			return
		}
		log.Printf("migration bridge consumer reconnecting in %s", backoff)
		b.reconnects.Add(1)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (b *Bridge) consume(ctx context.Context, reader *kafka.Reader) {
	// Commit a bounded batch instead of synchronously committing every CDC row.
	// The source topic is a single-partition append-only stream; committing each
	// snapshot row turns recovery into a network round-trip per row. A message is
	// added to this batch only after it was ignored safely, published, or sent to
	// the DLQ, so a process crash still causes only an idempotent redelivery.
	const commitBatchSize = 500
	const publishBatchSize = 100
	const publishBatchWait = 200 * time.Millisecond
	acked := make([]kafka.Message, 0, commitBatchSize)
	pendingEvents := make([]events.Envelope, 0, commitBatchSize)
	pendingMessages := make([]kafka.Message, 0, commitBatchSize)
	var pendingSince time.Time
	commit := func() bool {
		if len(acked) == 0 {
			return true
		}
		if err := reader.CommitMessages(ctx, acked...); err != nil {
			log.Printf("migration bridge batch commit failed: %v", err)
			return false
		}
		acked = acked[:0]
		return true
	}
	flush := func() bool {
		if len(pendingEvents) == 0 {
			return true
		}
		// Publish a bounded batch and commit the source rows only after the
		// complete batch succeeds. This keeps at-least-once delivery and source
		// ordering while avoiding one Kafka round trip per Debezium snapshot row.
		publishErr := resilience.Retry(ctx, resilience.RetryPolicyFromEnv(), func(attemptCtx context.Context, attempt int) error {
			log.Printf("migration bridge publishing batch size=%d attempt=%d", len(pendingEvents), attempt)
			return b.publisher.(BatchPublisher).PublishBatch(attemptCtx, pendingEvents)
		})
		if publishErr != nil {
			b.publishErrors.Add(1)
			log.Printf("migration bridge publish batch failed size=%d: %v", len(pendingEvents), publishErr)
			return false
		}
		acked = append(acked, pendingMessages...)
		b.processed.Add(uint64(len(pendingEvents)))
		pendingEvents = pendingEvents[:0]
		pendingMessages = pendingMessages[:0]
		pendingSince = time.Time{}
		return commit()
	}
	for {
		// Bound the wait for the next source record so low-volume topics flush
		// partial batches. MaxWait on ReaderConfig only controls the broker fetch;
		// it does not make FetchMessage return to the application. The application
		// contract is therefore 100 records or 200 ms, whichever comes first.
		fetchCtx := ctx
		cancel := func() {}
		if !pendingSince.IsZero() {
			remaining := publishBatchWait - time.Since(pendingSince)
			if remaining <= 0 {
				if !flush() {
					return
				}
				continue
			}
			fetchCtx, cancel = context.WithTimeout(ctx, remaining)
		}
		message, err := reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				if !flush() {
					return
				}
				continue
			}
			flush()
			commit()
			log.Printf("migration bridge consumer stopped: %v", err)
			return
		}
		// PostgreSQL Debezium CDC emits a Kafka tombstone after a DELETE. The preceding
		// Debezium envelope carries the actual delete operation; the tombstone
		// only removes the compacted source-key state and is not an event.
		if value := bytes.TrimSpace(message.Value); len(value) == 0 || bytes.Equal(value, []byte("null")) {
			acked = append(acked, message)
			if len(acked) >= commitBatchSize {
				commit()
			}
			continue
		}
		var event events.Envelope
		var mapErr error
		traceParent := headerValue(message.Headers, "traceparent")
		traceState := headerValue(message.Headers, "tracestate")
		attempts := 0
		mapErr = resilience.Retry(ctx, resilience.RetryPolicyFromEnv(), func(attemptCtx context.Context, attempt int) error {
			attempts = attempt
			transportkafka.Consumed(message.Topic, message.Partition, message.Offset, attempt, message.Value)
			event, mapErr = b.mapper.Map(message.Value)
			if errors.Is(mapErr, ErrIgnoredChange) {
				return resilience.Permanent(mapErr)
			}
			if mapErr == nil {
				mapErr = b.validator.Validate(attemptCtx, event)
			}
			return mapErr
		})
		if mapErr != nil {
			if errors.Is(mapErr, ErrIgnoredChange) {
				acked = append(acked, message)
				if len(acked) >= commitBatchSize {
					commit()
				}
				continue
			}
			log.Printf("migration bridge rejected topic=%s partition=%d offset=%d: %v", message.Topic, message.Partition, message.Offset, mapErr)
			if b.publisher.DeadLetter(ctx, message, attempts, mapErr.Error()) == nil {
				_ = reader.CommitMessages(ctx, message)
			}
			continue
		}
		event.TraceParent = traceParent
		event.TraceState = traceState
		pendingEvents = append(pendingEvents, event)
		pendingMessages = append(pendingMessages, message)
		if pendingSince.IsZero() {
			pendingSince = time.Now()
		}
		// Flush bounded batches. A partial batch is flushed when the source reader
		// stops, so low-volume live traffic is not lost in memory.
		if len(pendingEvents) >= publishBatchSize && !flush() {
			return
		}
	}
}

func headerValue(headers []kafka.Header, key string) string {
	for _, header := range headers {
		if strings.EqualFold(strings.TrimSpace(header.Key), key) {
			return string(header.Value)
		}
	}
	return ""
}
