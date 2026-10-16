package keyspace

import (
	"fmt"
	"strings"
)

// Family is package-owned structural Redis vocabulary below one Fatline scope.
// It can materialize concrete resource keys from opaque identities and ACL
// patterns from the same structural prefix.
type Family struct {
	scope Scope
	parts []string
}

// Resource is one concrete resource address. Identity segments are encoded
// injectively; child segments remain package-owned structural vocabulary.
type Resource struct {
	family     Family
	identities []string
	children   []string
}

func validStructure(part string) bool {
	part = strings.TrimSpace(part)
	return part != "" && !strings.ContainsAny(part, ":%*?[]{} \t\r\n")
}

// NewFamily creates a structural resource family such as
// <scope>:logma:pubsub:channel. Structural segments are never escaped: callers
// must supply package-owned, pattern-safe vocabulary.
func NewFamily(scope Scope, parts ...string) (Family, error) {
	if _, err := ParseScope(string(scope)); err != nil {
		return Family{}, err
	}
	if len(parts) == 0 {
		return Family{}, fmt.Errorf("resource family requires at least one structural segment")
	}
	cleaned := make([]string, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if !validStructure(part) {
			return Family{}, fmt.Errorf("invalid resource family segment %q", part)
		}
		cleaned[i] = part
	}
	return Family{scope: scope, parts: cleaned}, nil
}

// Resource materializes opaque identities beneath a family. Empty identities
// are rejected rather than silently becoming a sentinel such as "unknown".
func (f Family) Resource(identities ...string) (Resource, error) {
	if len(identities) == 0 {
		return Resource{}, fmt.Errorf("resource requires at least one identity")
	}
	encoded := make([]string, len(identities))
	for i, identity := range identities {
		identity = strings.TrimSpace(identity)
		if identity == "" {
			return Resource{}, fmt.Errorf("resource identity %d is empty", i)
		}
		encoded[i] = encodeIdentity(identity)
	}
	return Resource{family: f, identities: encoded}, nil
}

// Key returns the structural family prefix without a concrete identity. It is
// primarily useful for registry resources and provider-owned fixed keys.
func (f Family) Key() string {
	parts := make([]string, 0, 1+len(f.parts))
	parts = append(parts, string(f.scope))
	parts = append(parts, f.parts...)
	return strings.Join(parts, ":")
}

// KeyPattern renders a Redis key ACL pattern for this exact structural family.
func (f Family) KeyPattern() string { return "~" + f.Key() + ":*" }

// ReadPattern renders a Redis read-only key ACL pattern for this family.
func (f Family) ReadPattern() string { return "%R~" + f.Key() + ":*" }

// WritePattern renders a Redis write-only key ACL pattern for this family.
func (f Family) WritePattern() string { return "%W~" + f.Key() + ":*" }

// ChannelPattern renders a Redis Pub/Sub ACL pattern from the same structural
// family grammar. Domain Channel resources and transport topics remain separate
// concepts; callers choose the appropriate family for each.
func (f Family) ChannelPattern() string { return "&" + f.Key() + ":*" }

// Child appends package-owned structural vocabulary after the opaque identity.
// This supports relationship/index keys such as channel:<id>:subscribers.
func (r Resource) Child(parts ...string) (Resource, error) {
	if len(parts) == 0 {
		return r, nil
	}
	children := append([]string{}, r.children...)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if !validStructure(part) {
			return Resource{}, fmt.Errorf("invalid resource child segment %q", part)
		}
		children = append(children, part)
	}
	r.children = children
	return r, nil
}

// Key returns one concrete scoped resource key.
func (r Resource) Key() string {
	parts := make([]string, 0, 1+len(r.family.parts)+len(r.identities)+len(r.children))
	parts = append(parts, string(r.family.scope))
	parts = append(parts, r.family.parts...)
	parts = append(parts, r.identities...)
	parts = append(parts, r.children...)
	return strings.Join(parts, ":")
}

// encodeIdentity is the canonical v2 opaque Redis-segment encoding. The
// unescaped alphabet is deliberately narrow and stable. Percent is escaped
// first so the representation is injective.
func encodeIdentity(value string) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}
