package bridge

import (
	"context"
	"fmt"
	"strings"
	"time"

	commonevents "github.com/ofm-microservices/ofm-common/pkg/events"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"github.com/segmentio/kafka-go"
)

// ReplayDLQ republishes replayable DLQ envelopes to their original topics.
// The source DLQ offset is committed only after the original event has been
// published, so an interrupted replay can safely be resumed.
func ReplayDLQ(ctx context.Context, brokers, dlqTopic, groupID string, limit int) (int, error) {
	if strings.TrimSpace(brokers) == "" || strings.TrimSpace(dlqTopic) == "" || strings.TrimSpace(groupID) == "" {
		return 0, fmt.Errorf("replay configuration is incomplete")
	}
	if limit <= 0 {
		limit = 100
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: strings.Split(brokers, ","), Topic: dlqTopic, GroupID: groupID, StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 10 << 20, MaxWait: 50 * time.Millisecond})
	defer reader.Close()
	writers := make(map[string]*kafka.Writer)
	defer func() {
		for _, writer := range writers {
			_ = writer.Close()
		}
	}()
	replayed := 0
	for replayed < limit {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return replayed, ctx.Err()
			}
			return replayed, err
		}
		record, err := resilience.UnmarshalDLQ(message.Value)
		if err != nil || strings.TrimSpace(record.OriginalTopic) == "" {
			return replayed, fmt.Errorf("decode DLQ record at offset %d: %w", message.Offset, err)
		}
		writer := writers[record.OriginalTopic]
		if writer == nil {
			writer = &kafka.Writer{Addr: kafka.TCP(strings.Split(brokers, ",")[0]), Topic: record.OriginalTopic, Balancer: &kafka.Hash{}}
			writers[record.OriginalTopic] = writer
		}
		value, _, err := commonevents.Wrap(record.OriginalTopic, record.OriginalValue)
		if err != nil {
			return replayed, fmt.Errorf("wrap replay event %s: %w", record.EventID, err)
		}
		if err := writer.WriteMessages(ctx, kafka.Message{Key: record.OriginalKey, Value: value}); err != nil {
			return replayed, fmt.Errorf("replay event %s: %w", record.EventID, err)
		}
		if err := reader.CommitMessages(ctx, message); err != nil {
			return replayed, fmt.Errorf("commit replay offset %d: %w", message.Offset, err)
		}
		replayed++
	}
	return replayed, nil
}
