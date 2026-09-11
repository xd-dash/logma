package fatline

import "testing"

func TestAuthPolicyDigestNormalizesSets(t *testing.T) {
	left := AuthPolicy{Version: 1, Actions: []string{"deploy", "read"}, Resources: []string{"xd-dash/smoke"}}
	right := AuthPolicy{Version: 1, Actions: []string{"read", "deploy", "read"}, Resources: []string{"xd-dash/smoke"}}
	ld, err := left.Digest()
	if err != nil { t.Fatal(err) }
	rd, err := right.Digest()
	if err != nil { t.Fatal(err) }
	if ld != rd { t.Fatalf("normalized digests differ: %s != %s", ld, rd) }
}

func TestAuthPolicyAllowsOnlyNarrowing(t *testing.T) {
	parent := AuthPolicy{Version: 1, Actions: []string{"read", "deploy"}, Resources: []string{"xd-dash/smoke", "xd-dash/logma"}, Audiences: []string{"fatline"}}
	child := AuthPolicy{Version: 1, Actions: []string{"deploy"}, Resources: []string{"xd-dash/smoke"}, Audiences: []string{"fatline"}}
	if err := parent.Allows(child); err != nil { t.Fatal(err) }
	child.Actions = append(child.Actions, "admin")
	if err := parent.Allows(child); err == nil { t.Fatal("expected authority widening to fail") }
}

func TestBindingKeyIsOrgScoped(t *testing.T) {
	policy := AuthPolicy{Version: 1, Actions: []string{"invoke"}, Resources: []string{"webhook"}}
	digest, err := policy.Digest()
	if err != nil { t.Fatal(err) }
	binding := Binding{OrgID: "xd-dash", TenantID: "probot-runtime", ServiceID: "webhook", AuthProfile: "claims-v1", AuthPolicyDigest: digest, RateProfile: "lifecycle", RatePolicyCode: "2305843009213693952", Revision: 1}
	key, err := binding.RedisKey()
	if err != nil { t.Fatal(err) }
	if key != "xd-dash:fatline:auth:binding:service:probot-runtime:webhook" { t.Fatalf("unexpected key %q", key) }
}
