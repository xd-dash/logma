package pubsubmodel

import (
	"context"
	"testing"
)

func TestDeleteGraphResourcesRequireIdentity(t *testing.T) {
	store := &RedisStore{}
	ctx := context.Background()

	cases := map[string]func() error{
		"subscriber": func() error { return store.DeleteSubscriber(ctx, " ") },
		"publisher":  func() error { return store.DeletePublisher(ctx, "") },
		"callback":   func() error { return store.DeleteCallback(ctx, " ") },
		"channel":    func() error { return store.DeleteChannel(ctx, "") },
		"group":      func() error { return store.DeleteSubscriptionGroup(ctx, " ") },
	}

	for name, run := range cases {
		if err := run(); err == nil {
			t.Fatalf("%s deletion should require an identity", name)
		}
	}
}

func TestNormalizeIdentityTrimsWhitespace(t *testing.T) {
	if got := normalizeIdentity("  subscriber-1  "); got != "subscriber-1" {
		t.Fatalf("normalizeIdentity() = %q, want subscriber-1", got)
	}
}
