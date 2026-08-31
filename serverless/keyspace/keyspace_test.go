package keyspace

import "testing"

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

func TestRejectPatternCapableScope(t *testing.T) {
	for _, bad := range []string{"dev:*", "dev?x", "dev{x}", "dev x"} {
		if _, err := ParseScope(bad); err == nil { t.Fatalf("ParseScope(%q) succeeded", bad) }
	}
}
