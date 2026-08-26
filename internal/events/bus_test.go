package events

import (
	"context"
	"testing"
)

type memoryVersions map[string]uint64

func (m memoryVersions) CurrentVersion(_ context.Context, tenant, key string) (uint64, error) {
	return m[tenant+"\x00"+key], nil
}

func (m memoryVersions) CommitVersion(_ context.Context, tenant, key string, version uint64) error {
	m[tenant+"\x00"+key] = version
	return nil
}

func TestApplyIfNewer(t *testing.T) {
	store := memoryVersions{"logma\x00secret": 16}
	applied := 0
	event := Event{Tenant: "logma", Key: "secret", Version: 17}
	ok, err := ApplyIfNewer(context.Background(), store, event, func(context.Context, Event) error {
		applied++
		return nil
	})
	if err != nil || !ok || applied != 1 {
		t.Fatalf("apply=%v err=%v applied=%d", ok, err, applied)
	}
	ok, err = ApplyIfNewer(context.Background(), store, event, func(context.Context, Event) error {
		applied++
		return nil
	})
	if err != nil || ok || applied != 1 {
		t.Fatalf("duplicate apply=%v err=%v applied=%d", ok, err, applied)
	}
}
