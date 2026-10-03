# OFM Migration Bridge

## Purpose

The Migration Bridge transfers committed migration events between the CDC/Kafka backbone and migration consumers. It is infrastructure for monolith projection and recovery, not a business service. Status: active migration component.

## Boundaries and flow

The bridge consumes configured Kafka topics, validates and transforms event envelopes, and publishes the target migration event. It does not own service business data or monolith domain rules.

The source outbox transaction commits first. Debezium publishes the CDC record to Kafka. The bridge forwards it, and the target consumer acknowledges only after its projection and mapping transaction commits. Retry, dead-letter, duplicate, and partition behavior must remain observable.

## Configuration

.env.example should explain Kafka brokers/topics, consumer groups, retry/dead-letter settings, database/bridge endpoints, logging, and OpenTelemetry. Broker values select source and target topics; group values control partition ownership; retry values control replay behavior. No credentials belong in Git.

## Local development

    go test ./...
    go run ./cmd/migration-bridge

## Build and operations

Dockerfile builds ofm/migration-bridge:<tag>. Deployment manifests and Kafka/Debezium infrastructure remain in ofm-infra. Diagnose source CDC, Kafka partition lag, bridge consumer logs, retry/DLQ records, and target projection acknowledgments together.

