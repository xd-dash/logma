package keyspace

import "testing"

func TestLogmaPubSubGraphProviderIsResourceFamilyScoped(t *testing.T) {
	scope, _ := ParseScope("dev-safe")
	req, err := CompileRedisRequirements(scope, Grant{
		Capability: CapabilityLogmaPubSubGraph,
		Access:     AccessRead | AccessWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(req.KeyPatterns) != 1 || req.KeyPatterns[0] != "~dev-safe:logma:pubsub:*" {
		t.Fatalf("KeyPatterns=%v", req.KeyPatterns)
	}
	if len(req.ChannelPatterns) != 0 {
		t.Fatalf("graph storage unexpectedly granted channel authority: %v", req.ChannelPatterns)
	}

	set := map[string]bool{}
	for _, command := range req.Commands {
		set[command] = true
	}
	for _, required := range []string{
		"ping", "hello", "client",
		"hget", "hgetall", "hexists", "hset", "hdel",
		"smembers", "scard", "sadd", "srem", "exists", "del",
		"watch", "unwatch", "multi", "exec", "discard",
	} {
		if !set[required] {
			t.Fatalf("Redis requirements missing %q: %v", required, req.Commands)
		}
	}
	for _, forbidden := range []string{
		"fcall", "eval", "eval_ro", "function", "acl", "config", "module", "shutdown", "publish", "subscribe",
	} {
		if set[forbidden] {
			t.Fatalf("graph storage unexpectedly grants %q", forbidden)
		}
	}
}

func TestLogmaPubSubGraphReadOnlyProviderDoesNotGrantMutation(t *testing.T) {
	scope, _ := ParseScope("dev-safe")
	req, err := CompileRedisRequirements(scope, Grant{
		Capability: CapabilityLogmaPubSubGraph,
		Access:     AccessRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	set := map[string]bool{}
	for _, command := range req.Commands {
		set[command] = true
	}
	for _, forbidden := range []string{"hset", "hdel", "sadd", "srem", "del", "watch", "multi", "exec"} {
		if set[forbidden] {
			t.Fatalf("read-only graph grant contains %q", forbidden)
		}
	}
}

func TestLogmaPubSubGraphWriteOnlyProviderContainsMutationPrerequisites(t *testing.T) {
	scope, _ := ParseScope("dev-safe")
	req, err := CompileRedisRequirements(scope, Grant{
		Capability: CapabilityLogmaPubSubGraph,
		Access:     AccessWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	set := map[string]bool{}
	for _, command := range req.Commands {
		set[command] = true
	}
	for _, required := range []string{"hget", "smembers", "scard", "exists", "watch", "multi", "exec", "hset", "sadd", "srem", "del"} {
		if !set[required] {
			t.Fatalf("write-only graph requirements missing mutation prerequisite %q: %v", required, req.Commands)
		}
	}
	if set["hgetall"] {
		t.Fatalf("write-only graph grant unexpectedly contains independent read command hgetall")
	}
}

func TestRedisProviderFailsClosedForUnsupportedCapabilityOrAccess(t *testing.T) {
	scope, _ := ParseScope("dev-safe")
	if _, err := CompileRedisRequirements(scope, Grant{Capability: Capability("unknown"), Access: AccessRead}); err == nil {
		t.Fatal("unknown capability compiled")
	}
	if _, err := CompileRedisRequirements(scope, Grant{Capability: CapabilityLogmaPubSubGraph, Access: AccessInvoke}); err == nil {
		t.Fatal("unsupported graph access compiled")
	}
}
