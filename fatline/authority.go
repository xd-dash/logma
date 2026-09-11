package fatline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
)

type AuthCapability uint32

const (
	AuthCapabilityPrincipal AuthCapability = 1 << iota
	AuthCapabilityClaims
	AuthCapabilityRoles
	AuthCapabilityResource
	AuthCapabilityDelegation
	AuthCapabilityExpiry
	AuthCapabilityAudience
	AuthCapabilityNetwork
)

type AuthProfile struct {
	ID           string         `json:"id"`
	Capabilities AuthCapability `json:"capabilities"`
}

var AuthProfiles = map[string]AuthProfile{
	"minimal-v1":    {ID: "minimal-v1", Capabilities: AuthCapabilityPrincipal | AuthCapabilityResource},
	"jwt-v1":        {ID: "jwt-v1", Capabilities: AuthCapabilityPrincipal | AuthCapabilityClaims | AuthCapabilityExpiry | AuthCapabilityAudience | AuthCapabilityResource},
	"claims-v1":     {ID: "claims-v1", Capabilities: AuthCapabilityPrincipal | AuthCapabilityClaims | AuthCapabilityRoles | AuthCapabilityExpiry | AuthCapabilityAudience | AuthCapabilityResource},
	"capability-v1": {ID: "capability-v1", Capabilities: AuthCapabilityPrincipal | AuthCapabilityResource | AuthCapabilityDelegation | AuthCapabilityExpiry | AuthCapabilityAudience},
	"delegated-v1":  {ID: "delegated-v1", Capabilities: AuthCapabilityPrincipal | AuthCapabilityClaims | AuthCapabilityRoles | AuthCapabilityResource | AuthCapabilityDelegation | AuthCapabilityExpiry | AuthCapabilityAudience | AuthCapabilityNetwork},
}

type AuthPolicy struct {
	Version       uint8               `json:"version"`
	Actions       []string            `json:"actions,omitempty"`
	Resources     []string            `json:"resources,omitempty"`
	Audiences     []string            `json:"audiences,omitempty"`
	Roles         []string            `json:"roles,omitempty"`
	Claims        map[string][]string `json:"claims,omitempty"`
	Delegation    bool                `json:"delegation,omitempty"`
	RequireExpiry bool                `json:"require_expiry,omitempty"`
	Networks      []string            `json:"networks,omitempty"`
}

func (p AuthPolicy) Normalize() AuthPolicy {
	p.Actions = normalizeSet(p.Actions)
	p.Resources = normalizeSet(p.Resources)
	p.Audiences = normalizeSet(p.Audiences)
	p.Roles = normalizeSet(p.Roles)
	p.Networks = normalizeSet(p.Networks)
	if len(p.Claims) != 0 {
		claims := make(map[string][]string, len(p.Claims))
		for key, values := range p.Claims {
			claims[key] = normalizeSet(values)
		}
		p.Claims = claims
	}
	return p
}

func (p AuthPolicy) RequiredCapabilities() AuthCapability {
	var required AuthCapability
	if len(p.Resources) != 0 || len(p.Actions) != 0 { required |= AuthCapabilityResource }
	if len(p.Claims) != 0 { required |= AuthCapabilityClaims }
	if len(p.Roles) != 0 { required |= AuthCapabilityRoles }
	if len(p.Audiences) != 0 { required |= AuthCapabilityAudience }
	if p.Delegation { required |= AuthCapabilityDelegation }
	if p.RequireExpiry { required |= AuthCapabilityExpiry }
	if len(p.Networks) != 0 { required |= AuthCapabilityNetwork }
	return required
}

func (p AuthPolicy) Validate(profile AuthProfile) error {
	if p.Version != 1 { return fmt.Errorf("unsupported auth policy version %d", p.Version) }
	known, ok := AuthProfiles[profile.ID]
	if !ok || known.Capabilities != profile.Capabilities { return fmt.Errorf("unknown auth profile %q", profile.ID) }
	if required := p.RequiredCapabilities(); required&^profile.Capabilities != 0 {
		return fmt.Errorf("auth profile %q lacks capabilities %#x", profile.ID, required&^profile.Capabilities)
	}
	return nil
}

func (p AuthPolicy) Digest() (string, error) {
	payload, err := json.Marshal(p.Normalize())
	if err != nil { return "", err }
	return digestBytes(payload), nil
}

// Allows enforces monotonic narrowing. A child may remove authority but cannot
// introduce authority not present in its parent grant.
func (p AuthPolicy) Allows(child AuthPolicy) error {
	parent := p.Normalize()
	child = child.Normalize()
	for name, pair := range map[string][2][]string{
		"actions": {parent.Actions, child.Actions}, "resources": {parent.Resources, child.Resources},
		"audiences": {parent.Audiences, child.Audiences}, "roles": {parent.Roles, child.Roles},
		"networks": {parent.Networks, child.Networks},
	} {
		if !subset(pair[1], pair[0]) { return fmt.Errorf("child %s exceed parent authority", name) }
	}
	if child.Delegation && !parent.Delegation { return errors.New("child delegation exceeds parent authority") }
	if child.RequireExpiry && !parent.RequireExpiry { return errors.New("child expiry requirement exceeds parent policy shape") }
	for key, values := range child.Claims {
		parentValues, ok := parent.Claims[key]
		if !ok || !subset(values, parentValues) { return fmt.Errorf("child claim %q exceeds parent authority", key) }
	}
	return nil
}

type OrgGrant struct {
	OrgID        string `json:"org_id"`
	Name         string `json:"name"`
	PolicyDigest string `json:"policy_digest"`
	Revision     uint64 `json:"revision"`
}

type Binding struct {
	OrgID            string `json:"org_id"`
	TenantID         string `json:"tenant_id"`
	ServiceID        string `json:"service_id,omitempty"`
	RouteID          string `json:"route_id,omitempty"`
	AuthProfile      string `json:"auth_profile"`
	AuthPolicyDigest string `json:"auth_policy_digest"`
	RateProfile      string `json:"rate_profile,omitempty"`
	RatePolicyCode   string `json:"rate_policy_code,omitempty"`
	Revision         uint64 `json:"revision"`
}

func (b Binding) Validate() error {
	for name, value := range map[string]string{"org_id": b.OrgID, "tenant_id": b.TenantID} {
		if err := validateSegment(value); err != nil { return fmt.Errorf("%s: %w", name, err) }
	}
	if b.ServiceID != "" {
		if err := validateSegment(b.ServiceID); err != nil { return fmt.Errorf("service_id: %w", err) }
	}
	if b.RouteID != "" {
		if b.ServiceID == "" { return errors.New("route_id requires service_id") }
		if err := validateSegment(b.RouteID); err != nil { return fmt.Errorf("route_id: %w", err) }
	}
	if _, ok := AuthProfiles[b.AuthProfile]; !ok { return fmt.Errorf("unknown auth_profile %q", b.AuthProfile) }
	if !validDigest(b.AuthPolicyDigest) { return errors.New("auth_policy_digest must be sha256:<64 hex chars>") }
	if (b.RateProfile == "") != (b.RatePolicyCode == "") { return errors.New("rate_profile and rate_policy_code must be supplied together") }
	if b.Revision == 0 { return errors.New("revision must be non-zero") }
	return nil
}

func (b Binding) Digest() (string, error) {
	if err := b.Validate(); err != nil { return "", err }
	payload, err := json.Marshal(b)
	if err != nil { return "", err }
	return digestBytes(payload), nil
}

// RedisKey is the mutable control-plane alias for the current binding at this
// tenant/service/route scope. Durable execution records must freeze Digest().
func (b Binding) RedisKey() (string, error) {
	if err := b.Validate(); err != nil { return "", err }
	parts := []string{b.OrgID, "fatline", "auth", "binding"}
	switch {
	case b.RouteID != "": parts = append(parts, "route", b.TenantID, b.ServiceID, b.RouteID)
	case b.ServiceID != "": parts = append(parts, "service", b.TenantID, b.ServiceID)
	default: parts = append(parts, "tenant", b.TenantID)
	}
	return strings.Join(parts, ":"), nil
}

func (b Binding) ArtifactRedisKey() (string, error) {
	digest, err := b.Digest()
	if err != nil { return "", err }
	return b.OrgID + ":fatline:auth:binding-artifact:" + strings.TrimPrefix(digest, "sha256:"), nil
}

func PolicyRedisKey(orgID, digest string) (string, error) {
	if err := validateSegment(orgID); err != nil { return "", err }
	if !validDigest(digest) { return "", errors.New("invalid policy digest") }
	return orgID + ":fatline:auth:policy:" + strings.TrimPrefix(digest, "sha256:"), nil
}

func RuntimeKey(orgID, family, tenantID string, identity ...string) (string, error) {
	parts := []string{orgID, "fatline", "runtime", family, tenantID}
	parts = append(parts, identity...)
	for _, part := range parts { if err := validateSegment(part); err != nil { return "", err } }
	return strings.Join(parts, ":"), nil
}

func digestBytes(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 { return false }
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func validateSegment(value string) error {
	if value == "" { return errors.New("keyspace segment is empty") }
	if strings.ContainsAny(value, ":*?[]{}\\") || strings.IndexFunc(value, unicode.IsSpace) >= 0 {
		return errors.New("keyspace segment contains reserved Redis syntax or whitespace")
	}
	return nil
}

func normalizeSet(values []string) []string {
	if len(values) == 0 { return nil }
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" { continue }
		if _, ok := seen[value]; ok { continue }
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func subset(child, parent []string) bool {
	allowed := make(map[string]struct{}, len(parent))
	for _, value := range parent { allowed[value] = struct{}{} }
	for _, value := range child { if _, ok := allowed[value]; !ok { return false } }
	return true
}
