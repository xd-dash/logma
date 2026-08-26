package events

import "time"

// Event is the transport-neutral envelope for regional security/configuration changes.
// Transport and reconciliation are intentionally outside this package.
type Event struct {
	Type      string         `json:"type"`
	Tenant    string         `json:"tenant"`
	Region    string         `json:"region"`
	EventID   string         `json:"event_id"`
	Version   uint64         `json:"version"`
	Timestamp time.Time      `json:"timestamp"`
	Key       string         `json:"key,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

func (e Event) NewerThan(version uint64) bool {
	return e.Version > version
}
