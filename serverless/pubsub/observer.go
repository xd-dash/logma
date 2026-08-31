package pubsub

import (
	"context"
	"encoding/json"
	"time"
)

// ObservabilityEvent is the bounded, non-authoritative event contract exposed by
// the serverless runtime. Observers may mirror these events to external systems,
// but they must never become publication, lifecycle, or teardown authority.
type ObservabilityEvent struct {
	Time       time.Time       `json:"time"`
	Kind       string          `json:"kind"`
	Phase      string          `json:"phase"`
	Status     string          `json:"status,omitempty"`
	Namespace  string          `json:"namespace,omitempty"`
	InstanceID string          `json:"instance_id,omitempty"`
	RequestID  string          `json:"request_id,omitempty"`
	Channel    string          `json:"channel,omitempty"`
	Policy     string          `json:"policy,omitempty"`
	PolicyCode uint64          `json:"policy_code,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// Observer receives best-effort runtime events. Implementations should make
// Observe cheap and non-blocking; failures must not affect the data plane.
type Observer interface {
	Observe(context.Context, ObservabilityEvent)
}

func observe(observer Observer, ctx context.Context, event ObservabilityEvent) {
	if observer == nil {
		return
	}
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	observer.Observe(ctx, event)
}
