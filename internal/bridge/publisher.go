package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/migration/events"
	transportkafka "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	"github.com/segmentio/kafka-go"
)

// Publisher publishes canonical events and replayable failures to Kafka.
type Publisher interface {
	Publish(ctx context.Context, event events.Envelope) error
	DeadLetter(ctx context.Context, message kafka.Message, deliveryCount int, reason string) error
	Close() error
}

// BatchPublisher publishes already mapped events in fewer Kafka round trips.
type BatchPublisher interface {
	PublishBatch(context.Context, []events.Envelope) error
}

type publisher struct {
	canonical *kafka.Writer
	dlq       *kafka.Writer
	brokers   string
	prefix    string
	topics    map[string]struct{}
	topicsMu  sync.Mutex
}

// NewPublisher creates Kafka writers for canonical events and the DLQ.
func NewPublisher(brokers, deadLetter, prefix string) (Publisher, error) {
	if brokers == "" || deadLetter == "" || prefix == "" {
		return nil, fmt.Errorf("Kafka publisher configuration is incomplete")
	}
	return &publisher{
		canonical: &kafka.Writer{Addr: kafka.TCP(brokers), Balancer: &kafka.Hash{}, BatchSize: 100, BatchTimeout: 50 * time.Millisecond, WriteTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second},
		dlq:       &kafka.Writer{Addr: kafka.TCP(brokers), Topic: deadLetter, Balancer: &kafka.Hash{}, BatchSize: 100, BatchTimeout: 50 * time.Millisecond, WriteTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second},
		brokers:   brokers,
		prefix:    prefix,
		topics:    make(map[string]struct{}),
	}, nil
}

func (p *publisher) Publish(ctx context.Context, event events.Envelope) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	transportkafka.Published(event.EventType, payload)
	return p.PublishBatch(ctx, []events.Envelope{event})
}

// PublishBatch publishes all events grouped by destination topic.
func (p *publisher) PublishBatch(ctx context.Context, eventsToPublish []events.Envelope) error {
	messages := make(map[string][]kafka.Message)
	for _, event := range eventsToPublish {
		payload, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encode canonical event: %w", err)
		}
		topic := p.prefix + "." + event.EventType
		// Recovery commands are emitted from the monolith outbox through CDC and
		// must retain the stable command topic consumed by the recovery workers.
		if strings.HasPrefix(event.EventType, "recovery.") {
			// Route recovery commands to an aggregate-specific topic so a service
			// does not scan every other service's migration backlog. Consumers still
			// validate aggregate_type defensively at their application boundary.
			aggregate := strings.ToLower(strings.TrimSpace(event.AggregateType))
			switch aggregate {
			case "gigs":
				aggregate = "gig"
			case "users":
				aggregate = "user"
			case "order", "orders":
				// Order recovery uses the service's canonical singular topic.
				aggregate = "order"
			case "reviews":
				aggregate = "review"
			case "payments", "payment_intents", "payment_releases", "connect_accounts":
				aggregate = "payment"
			}
			topic = "migration.recovery.commands." + aggregate
		}
		if err := p.ensureTopic(ctx, topic); err != nil {
			return err
		}
		// Recovery commands form a workflow, not independent CRUD events. The
		// create command intentionally has no aggregate ID yet, while subsequent
		// commands do. Different keys would route them to different partitions
		// and allow publish/update to overtake create. Keep one stable key per
		// recovery stream so CDC order is preserved end-to-end.
		key := event.AggregateID
		if strings.HasPrefix(event.EventType, "recovery.") {
			// Recovery envelopes produced by the fallback outbox historically use
			// command_id as aggregate_id. Route by the resource UUID embedded in
			// the command path instead, so different gigs can be processed in
			// parallel while all commands for one gig stay ordered.
			if path := strings.TrimSpace(event.CommandPath); path != "" {
				if marker := strings.Index(path, "/gigs/"); marker >= 0 {
					value := path[marker+len("/gigs/"):]
					if slash := strings.IndexByte(value, '/'); slash >= 0 {
						value = value[:slash]
					}
					// A create-draft route has no aggregate UUID yet. Using the
					// literal "drafts" would funnel every create into one Kafka
					// partition and serialize the entire recovery stream.
					if value != "" && value != "drafts" {
						key = value
					}
				}
			}
			if strings.HasSuffix(strings.TrimSpace(event.CommandPath), "/gigs/drafts") {
				key = event.CommandID
			}
			if key == "" {
				key = topic
			}
		} else if key == "" {
			key = event.CommandID
		}
		if key == "" {
			key = event.EventID
		}
		headers := []kafka.Header{{Key: "event-type", Value: []byte(event.EventType)}, {Key: "event-schema-version", Value: []byte(fmt.Sprint(event.SchemaVersion))}}
		if event.TraceParent != "" {
			headers = append(headers, kafka.Header{Key: "traceparent", Value: []byte(event.TraceParent)})
		}
		if event.TraceState != "" {
			headers = append(headers, kafka.Header{Key: "tracestate", Value: []byte(event.TraceState)})
		}
		messages[topic] = append(messages[topic], kafka.Message{Topic: topic, Key: []byte(key), Value: payload, Headers: headers})
	}
	for topic, batch := range messages {
		log.Printf("migration bridge publishing batch topic=%s size=%d", topic, len(batch))
		if err := p.canonical.WriteMessages(ctx, batch...); err != nil {
			return fmt.Errorf("publish migration event batch topic=%s: %w", topic, err)
		}
	}
	return nil
}

// DeadLetter preserves invalid input and its reason for operator replay.
func (p *publisher) DeadLetter(ctx context.Context, message kafka.Message, deliveryCount int, reason string) error {
	if err := p.ensureTopic(ctx, p.dlq.Topic); err != nil {
		return err
	}
	return p.dlq.WriteMessages(ctx, deadLetterMessage(message, deliveryCount, reason))
}

func deadLetterMessage(message kafka.Message, deliveryCount int, reason string) kafka.Message {
	return kafka.Message{
		Key:   message.Key,
		Value: message.Value,
		Headers: []kafka.Header{
			{Key: "original-topic", Value: []byte(message.Topic)},
			{Key: "original-partition", Value: []byte(fmt.Sprint(message.Partition))},
			{Key: "original-offset", Value: []byte(fmt.Sprint(message.Offset))},
			{Key: "delivery-count", Value: []byte(fmt.Sprint(deliveryCount))},
			{Key: "dead-letter-reason", Value: []byte(reason)},
		},
	}
}

func (p *publisher) ensureTopic(ctx context.Context, topic string) error {
	p.topicsMu.Lock()
	defer p.topicsMu.Unlock()
	if _, ok := p.topics[topic]; ok {
		return nil
	}
	conn, err := kafka.DialContext(ctx, "tcp", p.brokers)
	if err != nil {
		return fmt.Errorf("connect to Kafka for topic provisioning: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ReadPartitions(topic); err != nil {
		if err := conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 4, ReplicationFactor: 1}); err != nil {
			return fmt.Errorf("provision Kafka topic %s: %w", topic, err)
		}
	}
	p.topics[topic] = struct{}{}
	return nil
}

func (p *publisher) Close() error { return errors.Join(p.canonical.Close(), p.dlq.Close()) }
