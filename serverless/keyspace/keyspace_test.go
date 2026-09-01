package keyspace

import (
	"reflect"
	"testing"
)

func TestScopeFirstNamesAndPatterns(t *testing.T) {
	s, err := ParseScope("dev-46c0018509a9a00f")
	if err != nil { t.Fatal(err) }
	if got, want := s.Name("news", "item", "globenewswire"), "dev-46c0018509a9a00f:news:item:globenewswire"; got != want { t.Fatalf("Name=%q want %q", got, want) }
	if got, want := s.Prefix("news:item:globenewswire"), "dev-46c0018509a9a00f:news:item:globenewswire"; got != want { t.Fatalf("Prefix=%q want %q", got, want) }
	if got, want := s.KeyPattern(), "~dev-46c0018509a9a00f:*"; got != want { t.Fatalf("KeyPattern=%q want %q", got, want) }
	if got, want := s.ChannelPattern(), "&dev-46c0018509a9a00f:*"; got != want { t.Fatalf("ChannelPattern=%q want %q", got, want) }
	w := Worker{Scope:s, Subsystem:"news"}
	if got, want := w.KeyPattern(), "~dev-46c0018509a9a00f:news:*"; got != want { t.Fatalf("worker KeyPattern=%q want %q", got, want) }
	if got, want := w.ChannelPattern(), "&dev-46c0018509a9a00f:news:*"; got != want { t.Fatalf("worker ChannelPattern=%q want %q", got, want) }
}

func TestNewsProfilePatterns(t *testing.T) {
	s, _ := ParseScope("dev-46c0018509a9a00f")
	got, err := NewsProfile(s).ACLPatterns()
	if err != nil { t.Fatal(err) }
	want := []string{
		"~dev-46c0018509a9a00f:logma:*",
		"~dev-46c0018509a9a00f:ratelimiter:*",
		"&dev-46c0018509a9a00f:news:*",
	}
	if !reflect.DeepEqual(got, want) { t.Fatalf("ACLPatterns=%v want %v", got, want) }
}

func TestNewsProfileACLRulesIncludeRuntimeAndFunctionCommands(t *testing.T) {
	s, _ := ParseScope("dev-46c0018509a9a00f")
	rules, err := NewsProfile(s).ACLRules()
	if err != nil { t.Fatal(err) }
	set := make(map[string]bool, len(rules))
	for _, rule := range rules { set[rule] = true }
	for _, required := range []string{
		"~dev-46c0018509a9a00f:logma:*",
		"~dev-46c0018509a9a00f:ratelimiter:*",
		"&dev-46c0018509a9a00f:news:*",
		"+multi", "+exec", "+fcall", "+publish", "+subscribe",
		"+hset", "+hget", "+hdel", "+hexists", "+expire",
		"+sadd", "+srem", "+del",
		"+type", "+time", "+zadd", "+zcard", "+zrange", "+zrangebyscore", "+zrem", "+zremrangebyscore", "+incr", "+pexpire",
	} {
		if !set[required] { t.Fatalf("ACLRules missing %q: %v", required, rules) }
	}
	for _, forbidden := range []string{"+eval", "+eval_ro", "+function", "+acl", "+config", "+module", "+shutdown"} {
		if set[forbidden] { t.Fatalf("ACLRules unexpectedly contains %q", forbidden) }
	}
}

func TestRejectPatternCapableScopeCapabilitiesAndCommands(t *testing.T) {
	for _, bad := range []string{"dev:*", "dev?x", "dev{x}", "dev x"} {
		if _, err := ParseScope(bad); err == nil { t.Fatalf("ParseScope(%q) succeeded", bad) }
	}
	s, _ := ParseScope("dev-safe")
	if _, err := (Profile{Scope:s, KeySubsystems:[]string{"news:*"}}).ACLPatterns(); err == nil {
		t.Fatal("wildcard-capable subsystem accepted")
	}
	for _, bad := range []string{"+get", "@all", "FUNCTION LOAD", "eval*", "GET"} {
		if _, err := (Profile{Scope:s, Commands:[]string{bad}}).ACLRules(); err == nil {
			t.Fatalf("invalid command capability %q accepted", bad)
		}
	}
}
