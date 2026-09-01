package lifecycle

import (
	"errors"
	"os"
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

func TestCreateDoesNotOverwriteExistingLifecycle(t *testing.T) {
	activated := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)
	first, err := NewRegistration(RegisterRequest{
		DeploymentID:    "create-once",
		PolicyName:      ratelimiter.LifecycleSmoke10M,
		ActivatedAt:     &activated,
		ShutdownChannel: "shutdown",
	}, activated)
	if err != nil {
		t.Fatal(err)
	}
	secondActivated := activated.Add(time.Minute)
	second, err := NewRegistration(RegisterRequest{
		DeploymentID:    "create-once",
		PolicyName:      ratelimiter.LifecycleSmoke10M,
		ActivatedAt:     &secondActivated,
		ShutdownChannel: "shutdown",
	}, secondActivated)
	if err != nil {
		t.Fatal(err)
	}

	store := FileStore{Dir: t.TempDir()}
	if err := store.Create(first); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(second); !errors.Is(err, os.ErrExist) {
		t.Fatalf("second create error = %v, want os.ErrExist", err)
	}
	loaded, err := store.Load(first.DeploymentID)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.ActivatedAt.Equal(first.ActivatedAt) || !loaded.Deadline.Equal(first.Deadline) {
		t.Fatalf("existing registration was changed: %#v", loaded)
	}
}

func TestExistingRegistrationRetryCannotRebaseActivation(t *testing.T) {
	activated := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)
	reg, err := NewRegistration(RegisterRequest{
		DeploymentID:    "retry",
		PolicyName:      ratelimiter.LifecycleSmoke10M,
		ActivatedAt:     &activated,
		ShutdownChannel: "shutdown",
		Metadata:        map[string]string{"state_locator": "state://exact"},
	}, activated)
	if err != nil {
		t.Fatal(err)
	}

	matches, err := reg.MatchesRequest(RegisterRequest{
		DeploymentID:    reg.DeploymentID,
		PolicyName:      ratelimiter.LifecycleSmoke10M,
		ShutdownChannel: reg.ShutdownChannel,
		Metadata:        map[string]string{"state_locator": "state://exact"},
	})
	if err != nil || !matches {
		t.Fatalf("idempotent retry = %v, %v", matches, err)
	}

	later := activated.Add(time.Minute)
	matches, err = reg.MatchesRequest(RegisterRequest{
		DeploymentID:    reg.DeploymentID,
		PolicyName:      ratelimiter.LifecycleSmoke10M,
		ActivatedAt:     &later,
		ShutdownChannel: reg.ShutdownChannel,
		Metadata:        map[string]string{"state_locator": "state://exact"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if matches {
		t.Fatal("retry with changed activation unexpectedly matched")
	}
}
