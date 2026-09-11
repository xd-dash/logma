package fatline

import (
	"strings"
	"testing"
)

func TestAuthPolicyDigestNormalizesSets(t *testing.T) {
	left := AuthPolicy{
		Version:   1,
		Actions:   []string{"deploy", "read"},
		Resources: []string{"xd-dash/smoke"},
	}
	right := AuthPolicy{
		Version:   1,
		Actions:   []string{"read", "deploy", "read"},
		Resources: []string{"xd-dash/smoke"},
	}
	ld, err := left.Digest()
	if err != nil {
		t.Fatal(err)
	}
	rd, err := right.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if ld != rd {
		t.Fatalf("normalized digests differ: %s != %s", ld, rd)
	}
}

func TestAuthPolicyAllowsOnlyNarrowing(t *testing.T) {
	parent := AuthPolicy{
		Version:   1,
		Actions:   []string{"read", "deploy"},
		Resources: []string{"xd-dash/smoke", "xd-dash/logma"},
		Audiences: []string{"fatline"},
	}
	child := AuthPolicy{
		Version:       1,
		Actions:       []string{"deploy"},
		Resources:     []string{"xd-dash/smoke"},
		Audiences:     []string{"fatline"},
		RequireExpiry: true,
	}
	if err := parent.Allows(child); err != nil {
		t.Fatal(err)
	}
	child.Actions = append(child.Actions, "admin")
	if err := parent.Allows(child); err == nil {
		t.Fatal("expected authority widening to fail")
	}
}

func TestAuthPolicyCannotRemoveParentExpiryRequirement(t *testing.T) {
	parent := AuthPolicy{
		Version:       1,
		Actions:       []string{"invoke"},
		Resources:     []string{"webhook"},
		RequireExpiry: true,
	}
	child := AuthPolicy{
		Version:   1,
		Actions:   []string{"invoke"},
		Resources: []string{"webhook"},
	}
	if err := parent.Allows(child); err == nil {
		t.Fatal("child unexpectedly removed parent expiry requirement")
	}
}

func TestBindingKeyIsOrgScoped(t *testing.T) {
	policy := AuthPolicy{
		Version:   1,
		Actions:   []string{"invoke"},
		Resources: []string{"webhook"},
	}
	digest, err := policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	binding := Binding{
		OrgID:            "xd-dash",
		TenantID:         "probot-runtime",
		ServiceID:        "webhook",
		AuthProfile:      "claims-v1",
		AuthPolicyDigest: digest,
		RateProfile:      "lifecycle",
		RatePolicyCode:   "2305843009213693952",
		Revision:         1,
	}
	key, err := binding.RedisKey()
	if err != nil {
		t.Fatal(err)
	}
	if key != "xd-dash:fatline:auth:binding:service:probot-runtime:webhook" {
		t.Fatalf("unexpected key %q", key)
	}
	artifact, err := binding.ArtifactRedisKey()
	if err != nil {
		t.Fatal(err)
	}
	if artifact == key {
		t.Fatal("binding alias unexpectedly equals immutable artifact key")
	}
}

func TestBindingRejectsNonCanonicalRateCodeAndDigest(t *testing.T) {
	binding := Binding{
		OrgID:            "xd-dash",
		TenantID:         "smoke",
		AuthProfile:      "minimal-v1",
		AuthPolicyDigest: "sha256:" + strings.Repeat("a", 64),
		RateProfile:      "lifecycle",
		RatePolicyCode:   "not-a-number",
		Revision:         1,
	}
	for _, code := range []string{"not-a-number", "+1", "01", "18446744073709551616"} {
		binding.RatePolicyCode = code
		if err := binding.Validate(); err == nil {
			t.Fatalf("non-canonical rate policy code %q unexpectedly accepted", code)
		}
	}

	binding.RatePolicyCode = "18446744073709551615"
	if err := binding.Validate(); err != nil {
		t.Fatalf("max uint64 rate policy code rejected: %v", err)
	}

	binding.RatePolicyCode = "1"
	binding.AuthPolicyDigest = "sha256:" + strings.Repeat("A", 64)
	if err := binding.Validate(); err == nil {
		t.Fatal("non-canonical uppercase digest unexpectedly accepted")
	}
}
