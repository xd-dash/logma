package pubsub

import (
	"context"
	"testing"
	"time"
)

func TestLifecyclePoliciesArePackageOwned(t *testing.T) {
	timer, err := LifecycleSandboxTimer.config()
	if err != nil {
		t.Fatal(err)
	}
	if timer.timer != 3*time.Second || timer.tickEvery != 250*time.Millisecond || timer.maxPublishes != 0 {
		t.Fatalf("unexpected timer policy: %#v", timer)
	}

	total, err := LifecycleSandboxTotal.config()
	if err != nil {
		t.Fatal(err)
	}
	if total.maxPublishes != 3 || total.timer != 0 {
		t.Fatalf("unexpected total policy: %#v", total)
	}

	bounded, err := LifecycleSandboxBounded.config()
	if err != nil {
		t.Fatal(err)
	}
	if bounded.timer != 30*time.Second || bounded.maxPublishes != 64 {
		t.Fatalf("unexpected bounded policy: %#v", bounded)
	}

	news20m, err := LifecycleSandboxNews20M.config()
	if err != nil {
		t.Fatal(err)
	}
	if news20m.timer != 20*time.Minute || news20m.tickEvery != time.Second || news20m.maxPublishes != 0 {
		t.Fatalf("unexpected 20-minute news policy: %#v", news20m)
	}

	if _, err := LifecyclePolicy("request-controlled").config(); err == nil {
		t.Fatal("expected unknown lifecycle policy to fail")
	}
}

func TestConfigureDefaultWithLifecycle(t *testing.T) {
	rt := NewRuntime(nil)
	rt.ConfigureDefaultWithLifecycle(
		LifecycleSandboxBounded,
		func(context.Context) error { return nil },
		nil,
	)
	if rt.spec.Lifecycle != LifecycleSandboxBounded {
		t.Fatalf("lifecycle = %q", rt.spec.Lifecycle)
	}
}
