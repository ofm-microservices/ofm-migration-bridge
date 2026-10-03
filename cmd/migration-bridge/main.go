package main

import (
	"context"
	"database/sql"
	"log"
	"os/signal"
	"syscall"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-migration-bridge/internal/bridge"
	"github.com/ofm-microservices/ofm-migration-bridge/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	if _, loggerErr := logging.NewWithMode("migration-bridge", cfg.ObservabilityMode, cfg.ObservabilityMode, "info"); loggerErr != nil {
		log.Fatal(loggerErr)
	}
	if cfg.ReplayDLQ {
		replayed, replayErr := bridge.ReplayDLQ(context.Background(), cfg.KafkaBrokers, cfg.DeadLetter, cfg.ConsumerGroup+"-replay", cfg.ReplayLimit)
		if replayErr != nil {
			log.Fatal(replayErr)
		}
		log.Printf("replayed %d DLQ events", replayed)
		return
	}
	if cfg.RelayFailures {
		db, dbErr := sql.Open("pgx", cfg.DatabaseURL)
		if dbErr != nil {
			log.Fatal(dbErr)
		}
		defer db.Close()
		relayed, relayErr := bridge.RelayMigrationFailures(context.Background(), db, cfg.KafkaBrokers, cfg.DeadLetter, cfg.ReplayLimit)
		if relayErr != nil {
			log.Fatal(relayErr)
		}
		log.Printf("relayed %d monolith failures", relayed)
		return
	}
	publisher, err := bridge.NewPublisher(cfg.KafkaBrokers, cfg.DeadLetter, cfg.OutputPrefix)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()
	runCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	mapper := bridge.NewMapper()
	validator := bridge.NewRegistryValidator(cfg.SchemaRegistry)
	worker, err := bridge.New(cfg.KafkaBrokers, cfg.SourceTopics, cfg.ConsumerGroup, mapper, validator, publisher)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if metricsErr := worker.ServeMetrics(cfg.MetricsAddress); metricsErr != nil {
			log.Printf("bridge metrics server stopped: %v", metricsErr)
		}
	}()
	if runErr := worker.Run(runCtx); runErr != nil && runErr != context.Canceled {
		log.Fatal(runErr)
	}
}
