package lifecycle

import "testing"

func TestLifecycleKeyspaceUsesFatlineScope(t *testing.T) {
	keyspace, scope, err := lifecycleKeyspace("fatline-prod")
	if err != nil {
		t.Fatal(err)
	}
	if keyspace != "fatline-prod:logma:lifecycle" || scope != "fatline-prod" {
		t.Fatalf("lifecycleKeyspace() = (%q, %q), want scoped keyspace", keyspace, scope)
	}
}

func TestLifecycleKeyspaceRejectsRedisPatterns(t *testing.T) {
	if _, _, err := lifecycleKeyspace("fatline-prod:*"); err == nil {
		t.Fatal("lifecycleKeyspace accepted a pattern-capable scope")
	}
}

func TestScopedLifecycleRequiresScopedShutdownChannel(t *testing.T) {
	runtime := &Runtime{scope: "fatline-prod"}
	if err := runtime.validateShutdownChannel("fatline-prod:logma:shutdown"); err != nil {
		t.Fatalf("valid scoped channel rejected: %v", err)
	}
	if err := runtime.validateShutdownChannel("foreign:logma:shutdown"); err == nil {
		t.Fatal("foreign shutdown channel accepted")
	}
}
