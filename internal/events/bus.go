package events

import "context"

// Bus is the transport boundary for regional events. Implementations may use
// Redis Pub/Sub for fast notification, but durable reconciliation belongs to a
// transport with catch-up semantics.
type Bus interface {
	Publish(context.Context, Event) error
	Subscribe(context.Context, string, Handler) error
}

type Handler func(context.Context, Event) error

// VersionStore is the minimal reconciliation boundary. Consumers can reject
// stale or duplicate events without coupling event transport to state storage.
type VersionStore interface {
	CurrentVersion(context.Context, string, string) (uint64, error)
	CommitVersion(context.Context, string, string, uint64) error
}

func ApplyIfNewer(ctx context.Context, store VersionStore, event Event, apply Handler) (bool, error) {
	current, err := store.CurrentVersion(ctx, event.Tenant, event.Key)
	if err != nil {
		return false, err
	}
	if !event.NewerThan(current) {
		return false, nil
	}
	if err := apply(ctx, event); err != nil {
		return false, err
	}
	if err := store.CommitVersion(ctx, event.Tenant, event.Key, event.Version); err != nil {
		return false, err
	}
	return true, nil
}
