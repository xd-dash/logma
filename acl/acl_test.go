package acl

import (
	"slices"
	"testing"
)

func TestTenantRulesAreScopedAndDoNotGrantScriptingAdministration(t *testing.T) {
	policy, err := PolicyByName("tenant-functions")
	if err != nil {
		t.Fatal(err)
	}
	rules, err := RulesForTenant("acme", "secret", policy, true)
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"%RW~logma:tenant:acme:*",
		"&tenant:acme:*",
		"+publish",
		"+subscribe",
		"+fcall",
		"+fcall_ro",
	} {
		if !slices.Contains(rules, want) {
			t.Fatalf("rules missing %q: %#v", want, rules)
		}
	}

	for _, forbidden := range []string{
		"+eval",
		"+evalsha",
		"+function",
		"+function|load",
		"+acl",
		"+scan",
		"+keys",
		"+@all",
	} {
		if slices.Contains(rules, forbidden) {
			t.Fatalf("rules unexpectedly grant %q", forbidden)
		}
	}
}

func TestFunctionNamesAreTenantScoped(t *testing.T) {
	got, err := TenantFunctionName("acme", "normalize")
	if err != nil {
		t.Fatal(err)
	}
	if got != "logma_acme__normalize" {
		t.Fatalf("got %q", got)
	}
}
