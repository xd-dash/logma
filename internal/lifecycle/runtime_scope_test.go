package lifecycle

import "testing"

func TestLifecycleKeyspaceUsesFatlineScope(t *testing.T) {
	got, scope, err := lifecycleKeyspace("fatline-prod")
	if err != nil {
		t.Fatal(err)
	}
	if got != "fatline-prod:logma:lifecycle" || scope != "fatline-prod" {
		t.Fatalf("lifecycleKeyspace() = (%q, %q), want scoped keyspace", got, scope)
	}
}

func TestLifecycleKeyspaceRejectsInvalidScope(t *testing.T) {
	if _, _, err := lifecycleKeyspace("foreign scope"); err == nil {
		t.Fatal("lifecycleKeyspace accepted invalid scope")
	}
}

func TestLifecycleKeyspaceRetainsUnscopedCompatibility(t *testing.T) {
	got, scope, err := lifecycleKeyspace("")
	if err != nil {
		t.Fatal(err)
	}
	if got != defaultKeyspace || scope != "" {
		t.Fatalf("lifecycleKeyspace() = (%q, %q), want legacy default", got, scope)
	}
}

func TestValidateShutdownChannelRequiresMatchingScope(t *testing.T) {
	runtime := &Runtime{scope: "fatline-prod"}
	if err := runtime.validateShutdownChannel("fatline-prod:tenant:shutdown"); err != nil {
		t.Fatalf("scoped channel rejected: %v", err)
	}
	if err := runtime.validateShutdownChannel("other:tenant:shutdown"); err == nil {
		t.Fatal("foreign channel accepted")
	}
}
