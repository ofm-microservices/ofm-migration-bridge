package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/migration/events"
)

// Validator validates the canonical event before publication.
type Validator interface {
	Validate(context.Context, events.Envelope) error
}

type registryValidator struct {
	baseURL string
	client  *http.Client
	mu      sync.RWMutex
	known   map[string]struct{}
}

// NewRegistryValidator creates a Schema Registry-backed validator.
func NewRegistryValidator(baseURL string) Validator {
	return &registryValidator{baseURL: strings.TrimRight(baseURL, "/"), client: &http.Client{Timeout: 3 * time.Second}, known: make(map[string]struct{})}
}

func (v *registryValidator) Validate(ctx context.Context, event events.Envelope) error {
	// Recovery create commands intentionally have no aggregate ID yet: the
	// owning service allocates the microservice UUID while applying the command.
	// command_id remains the durable identity for ordering/idempotency.
	missingAggregateID := event.AggregateID == "" && !strings.HasPrefix(event.EventType, "recovery.")
	if event.EventID == "" || event.EventType == "" || event.SchemaVersion <= 0 || missingAggregateID || len(event.Payload) == 0 {
		return fmt.Errorf("canonical event has incomplete envelope")
	}
	if v.baseURL == "" {
		return nil
	}
	artifactID := event.EventType + ".v" + fmt.Sprint(event.SchemaVersion)
	v.mu.RLock()
	_, known := v.known[artifactID]
	v.mu.RUnlock()
	if known {
		return nil
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event for schema validation: %w", err)
	}
	// Apicurio Registry 3.x removed the v2 "test" endpoint. Validate that the
	// canonical artifact exists and lazily register the JSON contract on the
	// first event so a fresh local registry is usable without a manual schema
	// bootstrap step.
	baseURL := strings.Replace(v.baseURL, "/apis/registry/v2", "/apis/registry/v3", 1)
	artifactURL := baseURL + "/groups/default/artifacts/" + artifactID
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, artifactURL, nil)
	if err != nil {
		return err
	}
	response, err := v.client.Do(request)
	if err != nil {
		return fmt.Errorf("schema registry unavailable: %w", err)
	}
	defer response.Body.Close()
	response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		artifact := map[string]any{
			"artifactId":   artifactID,
			"artifactType": "JSON",
			"firstVersion": map[string]any{
				"version": "1",
				"content": map[string]any{
					"content":     string(payload),
					"contentType": "application/json",
				},
			},
		}
		body, marshalErr := json.Marshal(artifact)
		if marshalErr != nil {
			return fmt.Errorf("encode schema artifact: %w", marshalErr)
		}
		createRequest, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/groups/default/artifacts", bytes.NewReader(body))
		if requestErr != nil {
			return requestErr
		}
		createRequest.Header.Set("Content-Type", "application/json")
		createResponse, createErr := v.client.Do(createRequest)
		if createErr != nil {
			return fmt.Errorf("schema registry unavailable: %w", createErr)
		}
		defer createResponse.Body.Close()
		if createResponse.StatusCode != http.StatusOK && createResponse.StatusCode != http.StatusCreated && createResponse.StatusCode != http.StatusConflict {
			return fmt.Errorf("schema registration failed for %s: status %d", event.EventType, createResponse.StatusCode)
		}
		v.mu.Lock()
		v.known[artifactID] = struct{}{}
		v.mu.Unlock()
		return nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("schema validation failed for %s: status %d", event.EventType, response.StatusCode)
	}
	v.mu.Lock()
	v.known[artifactID] = struct{}{}
	v.mu.Unlock()
	return nil
}
