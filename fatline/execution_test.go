package fatline

import (
	"slices"
	"strings"
	"testing"

	"github.com/dash-xd/ratelimiter/redisacl/managed"
)

func TestCompileRedisExecutionUsesNarrowProfile(t *testing.T) {
	compiler, err := managed.New(managed.Config{
		AdminUser:      "logma-admin",
		UsernamePrefix: "logma-tenant-",
		KeyPrefix:      "logma:tenant:",
		ChannelPrefix:  "tenant:",
		FunctionPrefix: "logma_",
	})
	if err != nil {
		t.Fatal(err)
	}
	binding := Binding{
		OrgID:            "xd-dash",
		TenantID:         "world-17",
		ServiceID:        "logma",
		AuthProfile:      "capability-v1",
		AuthPolicyDigest: "sha256:" + strings.Repeat("a", 64),
		RedisACLProfile:  "publisher",
		Revision:         1,
	}
	identity, err := CompileRedisExecution(binding, compiler, "", "local-secret")
	if err != nil {
		t.Fatal(err)
	}
	if identity.Username != "logma-tenant-world-17" || identity.Profile != "publisher" {
		t.Fatalf("unexpected identity: %#v", identity)
	}
	if !slices.Contains(identity.Rules, "+publish") || slices.Contains(identity.Rules, "+subscribe") || slices.Contains(identity.Rules, "+acl") {
		t.Fatalf("publisher execution rules are not narrow: %#v", identity.Rules)
	}
}

func TestCompileRedisExecutionDoesNotInferGlobalIdentity(t *testing.T) {
	compiler, err := managed.New(managed.Config{AdminUser: "logma-admin"})
	if err != nil {
		t.Fatal(err)
	}
	binding := Binding{
		OrgID:            "xd-dash",
		TenantID:         "world-17",
		ServiceID:        "logma",
		AuthProfile:      "capability-v1",
		AuthPolicyDigest: "sha256:" + strings.Repeat("b", 64),
		RedisACLProfile:  "subscriber",
		Revision:         1,
	}
	identity, err := CompileRedisExecution(binding, compiler, "logma-local", "local-secret")
	if err != nil {
		t.Fatal(err)
	}
	if identity.Username != "logma-local" {
		t.Fatalf("unexpected username %q", identity.Username)
	}
	if strings.Contains(identity.Username, "ed25519:") || strings.Contains(identity.Username, "spiffe://") {
		t.Fatalf("local Redis identity leaked global-principal syntax: %q", identity.Username)
	}
}
