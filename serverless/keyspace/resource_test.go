package keyspace

import "testing"

func TestResourceIdentityEncodingIsInjectiveForAmbiguousInputs(t *testing.T) {
	scope, err := ParseScope("dev-safe")
	if err != nil {
		t.Fatal(err)
	}
	family, err := NewFamily(scope, "logma", "pubsub", "channel")
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]string{
		"abc-123": "dev-safe:logma:pubsub:channel:abc-123",
		"foo:bar": "dev-safe:logma:pubsub:channel:foo%3Abar",
		"foo%bar": "dev-safe:logma:pubsub:channel:foo%25bar",
		"foo bar": "dev-safe:logma:pubsub:channel:foo%20bar",
		"foo_bar": "dev-safe:logma:pubsub:channel:foo_bar",
		"unknown": "dev-safe:logma:pubsub:channel:unknown",
	}
	seen := map[string]string{}
	for identity, want := range cases {
		resource, err := family.Resource(identity)
		if err != nil {
			t.Fatalf("Resource(%q): %v", identity, err)
		}
		got := resource.Key()
		if got != want {
			t.Fatalf("Resource(%q).Key()=%q want %q", identity, got, want)
		}
		if prior, exists := seen[got]; exists {
			t.Fatalf("identity collision: %q and %q both map to %q", prior, identity, got)
		}
		seen[got] = identity
	}
}

func TestResourceRejectsEmptyIdentityAndUnsafeStructure(t *testing.T) {
	scope, _ := ParseScope("dev-safe")
	for _, parts := range [][]string{{}, {"logma:*"}, {"logma", "pub sub"}, {"logma", "pubsub", "channel:unsafe"}} {
		if _, err := NewFamily(scope, parts...); err == nil {
			t.Fatalf("NewFamily(%v) succeeded", parts)
		}
	}
	family, err := NewFamily(scope, "logma", "pubsub", "channel")
	if err != nil {
		t.Fatal(err)
	}
	for _, identity := range []string{"", " ", "\t"} {
		if _, err := family.Resource(identity); err == nil {
			t.Fatalf("Resource(%q) succeeded", identity)
		}
	}
}

func TestResourceChildrenAndFamilyPatternsShareGrammar(t *testing.T) {
	scope, _ := ParseScope("dev-safe")
	family, err := NewFamily(scope, "logma", "pubsub", "channel")
	if err != nil {
		t.Fatal(err)
	}
	resource, err := family.Resource("dev:global:events")
	if err != nil {
		t.Fatal(err)
	}
	child, err := resource.Child("subscribers")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := child.Key(), "dev-safe:logma:pubsub:channel:dev%3Aglobal%3Aevents:subscribers"; got != want {
		t.Fatalf("child Key=%q want %q", got, want)
	}
	if got, want := family.KeyPattern(), "~dev-safe:logma:pubsub:channel:*"; got != want {
		t.Fatalf("KeyPattern=%q want %q", got, want)
	}
	if got, want := family.ChannelPattern(), "&dev-safe:logma:pubsub:channel:*"; got != want {
		t.Fatalf("ChannelPattern=%q want %q", got, want)
	}
}

func TestIdentityEncodingDoesNotCollapseStructuralSeparators(t *testing.T) {
	scope, _ := ParseScope("dev-safe")
	family, _ := NewFamily(scope, "logma", "pubsub", "channel")

	one, _ := family.Resource("foo:subscribers")
	two, _ := family.Resource("foo")
	twoChild, _ := two.Child("subscribers")
	if one.Key() == twoChild.Key() {
		t.Fatalf("opaque identity collided with structural child: %q", one.Key())
	}
}
