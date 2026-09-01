package pubsub

import (
	"context"
	"testing"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
)

func TestLifecyclePoliciesCompileThroughEnergyModel(t *testing.T) {
	tests := []struct {
		policy       Policy
		timer        time.Duration
		maxPublishes int64
	}{
		{Policy3S, 3 * time.Second, 0},
		{Policy3Publishes, 0, 3},
		{Policy30S, 30 * time.Second, 0},
		{Policy30S64Publishes, 30 * time.Second, 64},
		{Policy5M, 5 * time.Minute, 0},
		{Policy20M, 20 * time.Minute, 0},
	}

	for _, test := range tests {
		cfg, err := test.policy.config()
		if err != nil {
			t.Fatalf("%s: %v", test.policy, err)
		}
		if cfg.timer != test.timer || cfg.maxPublishes != test.maxPublishes {
			t.Fatalf("%s: config=%#v", test.policy, cfg)
		}
		if cfg.code == 0 || cfg.energy == 0 {
			t.Fatalf("%s: missing compiled code/energy: %#v", test.policy, cfg)
		}
		decoded, err := ratelimiter.DecodePolicy(cfg.code)
		if err != nil {
			t.Fatalf("%s decode: %v", test.policy, err)
		}
		if decoded.EnergyCost() != cfg.energy {
			t.Fatalf("%s energy=%d decoded=%d", test.policy, cfg.energy, decoded.EnergyCost())
		}
	}

	if _, err := Policy("request-controlled").config(); err == nil {
		t.Fatal("expected unknown lifecycle policy to fail")
	}
}

func TestConfigureDefaultWithLifecycle(t *testing.T) {
	rt := NewRuntime(nil)
	rt.ConfigureDefaultWithLifecycle(
		Policy30S,
		func(context.Context) error { return nil },
		nil,
	)
	if rt.spec.Lifecycle != Policy30S {
		t.Fatalf("lifecycle = %q", rt.spec.Lifecycle)
	}
}
