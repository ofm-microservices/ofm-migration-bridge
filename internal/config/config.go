package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config contains the runtime configuration for the CDC bridge.
type Config struct {
	KafkaBrokers      string
	SourceTopics      string
	ConsumerGroup     string
	OutputPrefix      string
	DeadLetter        string
	SchemaRegistry    string
	ReplayDLQ         bool
	ReplayLimit       int
	RelayFailures     bool
	DatabaseURL       string
	MetricsAddress    string
	ObservabilityMode string
}

// Load reads bridge configuration from environment variables.
func Load() (Config, error) {
	cfg := Config{
		KafkaBrokers:      env("MIGRATION_KAFKA_BROKERS", "127.0.0.1:9092"),
		SourceTopics:      env("MIGRATION_KAFKA_SOURCE_TOPICS", "cdc.auth.auth_credentials"),
		ConsumerGroup:     env("MIGRATION_KAFKA_CONSUMER_GROUP", "migration-bridge"),
		OutputPrefix:      env("MIGRATION_EVENT_PREFIX", "migration"),
		DeadLetter:        env("MIGRATION_DEAD_LETTER_TOPIC", "migration.dead-letter"),
		SchemaRegistry:    env("MIGRATION_SCHEMA_REGISTRY_URL", "http://127.0.0.1:8084/apis/registry/v3"),
		ReplayDLQ:         Bool("MIGRATION_REPLAY_DLQ", false),
		ReplayLimit:       intEnv("MIGRATION_REPLAY_LIMIT", 100),
		RelayFailures:     Bool("MIGRATION_RELAY_FAILURES", false),
		DatabaseURL:       env("MIGRATION_FAILURE_DATABASE_URL", ""),
		MetricsAddress:    env("MIGRATION_METRICS_ADDRESS", ":9607"),
		ObservabilityMode: env("APP_OBSERVABILITY_MODE", "production"),
	}
	if cfg.KafkaBrokers == "" || cfg.SourceTopics == "" || cfg.ConsumerGroup == "" || cfg.OutputPrefix == "" || cfg.DeadLetter == "" {
		return Config{}, fmt.Errorf("migration bridge configuration contains an empty required value")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// Bool reads an optional boolean environment value.
func Bool(key string, fallback bool) bool {
	value, err := strconv.ParseBool(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return value
}

func intEnv(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
