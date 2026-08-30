package lifecycle

import (
	"testing"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
)

func TestNamedPolicyRegistrationPersistsAbsoluteDeadline(t *testing.T) {
	activated := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	reg, err := NewRegistration(RegisterRequest{
		DeploymentID:    "deploy-123",
		PolicyName:      ratelimiter.LifecycleSandbox1D,
		ActivatedAt:     &activated,
		ShutdownChannel: "lifecycle:shutdown",
		Metadata:        map[string]string{"terraform_state": "gs://example/state"},
	}, activated.Add(10*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if !reg.Deadline.Equal(activated.Add(24 * time.Hour)) {
		t.Fatalf("deadline = %s", reg.Deadline)
	}

	store := FileStore{Dir: t.TempDir()}
	if err := store.Save(reg); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].PolicyCode != reg.PolicyCode || !loaded[0].Deadline.Equal(reg.Deadline) {
		t.Fatalf("loaded registration = %#v", loaded)
	}
}

func TestExplicitPolicyCodeAndNamedPolicyResolveSameDeadline(t *testing.T) {
	activated := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	policy, err := ratelimiter.NamedLifecyclePolicy(ratelimiter.LifecycleSmoke1M)
	if err != nil {
		t.Fatal(err)
	}
	code, err := ratelimiter.EncodePolicy(policy)
	if err != nil {
		t.Fatal(err)
	}

	named, err := NewRegistration(RegisterRequest{
		DeploymentID:    "named",
		PolicyName:      ratelimiter.LifecycleSmoke1M,
		ActivatedAt:     &activated,
		ShutdownChannel: "shutdown",
	}, activated)
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := NewRegistration(RegisterRequest{
		DeploymentID:    "explicit",
		PolicyCode:      named.PolicyCode,
		ActivatedAt:     &activated,
		ShutdownChannel: "shutdown",
	}, activated.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if named.PolicyCode != explicit.PolicyCode || !named.Deadline.Equal(explicit.Deadline) {
		t.Fatalf("named=%#v explicit=%#v code=%d", named, explicit, uint64(code))
	}
}
