package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"github.com/segmentio/kafka-go"
)

// RelayMigrationFailures publishes monolith terminal failures to the DLQ and
// marks them published only after Kafka acknowledges the write.
func RelayMigrationFailures(ctx context.Context, db *sql.DB, brokers, topic string, limit int) (int, error) {
	if db == nil || brokers == "" || topic == "" {
		return 0, fmt.Errorf("failure relay configuration is incomplete")
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := db.QueryContext(ctx, `SELECT id, original_topic, original_partition, original_offset, original_key, original_value, attempts, error_class, error_message, failed_at FROM migration_projection_failures WHERE published_at IS NULL ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	writer := &kafka.Writer{Addr: kafka.TCP(brokers), Topic: topic, Balancer: &kafka.Hash{}}
	defer writer.Close()
	relayed := 0
	for rows.Next() {
		var id, partition, attempts int
		var offset int64
		var originalTopic, errorClass, errorMessage string
		var key, value []byte
		var failedAt time.Time
		if err := rows.Scan(&id, &originalTopic, &partition, &offset, &key, &value, &attempts, &errorClass, &errorMessage, &failedAt); err != nil {
			return relayed, err
		}
		payload, err := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: key, OriginalValue: value, OriginalTopic: originalTopic, OriginalPartition: partition, OriginalOffset: offset, Attempts: attempts, ErrorClass: errorClass, Error: errorMessage, FailedAt: failedAt})
		if err != nil {
			return relayed, err
		}
		if err := writer.WriteMessages(ctx, kafka.Message{Key: key, Value: payload}); err != nil {
			return relayed, err
		}
		if _, err := db.ExecContext(ctx, `UPDATE migration_projection_failures SET published_at=NOW() WHERE id=$1 AND published_at IS NULL`, id); err != nil {
			return relayed, err
		}
		relayed++
	}
	return relayed, rows.Err()
}
