package fatline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/xd-dash/prajapati/authz"
)

// OrgGrant and Binding remain Logma/Fatline control-plane artifacts. Generic
// authorization semantics live in Prajapati's public authz package.
type OrgGrant struct {
	OrgID        string `json:"org_id"`
	Name         string `json:"name"`
	PolicyDigest string `json:"policy_digest"`
	Revision     uint64 `json:"revision"`
}

func (g OrgGrant) Validate() error {
	if err := validateSegment(g.OrgID); err != nil {
		return fmt.Errorf("org_id: %w", err)
	}
	if err := validateSegment(g.Name); err != nil {
		return fmt.Errorf("name: %w", err)
	}
	if !validDigest(g.PolicyDigest) {
		return errors.New("policy_digest must be canonical sha256:<64 lowercase hex chars>")
	}
	if g.Revision == 0 {
		return errors.New("revision must be non-zero")
	}
	return nil
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
	RedisACLProfile  string `json:"redis_acl_profile,omitempty"`
	Revision         uint64 `json:"revision"`
}

func (b Binding) Validate() error {
	for name, value := range map[string]string{
		"org_id":    b.OrgID,
		"tenant_id": b.TenantID,
	} {
		if err := validateSegment(value); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	if b.ServiceID != "" {
		if err := validateSegment(b.ServiceID); err != nil {
			return fmt.Errorf("service_id: %w", err)
		}
	}
	if b.RouteID != "" {
		if b.ServiceID == "" {
			return errors.New("route_id requires service_id")
		}
		if err := validateSegment(b.RouteID); err != nil {
			return fmt.Errorf("route_id: %w", err)
		}
	}
	if _, ok := authz.ProfileFor(b.AuthProfile); !ok {
		return fmt.Errorf("unknown auth_profile %q", b.AuthProfile)
	}
	if !validDigest(b.AuthPolicyDigest) {
		return errors.New("auth_policy_digest must be canonical sha256:<64 lowercase hex chars>")
	}
	if (b.RateProfile == "") != (b.RatePolicyCode == "") {
		return errors.New("rate_profile and rate_policy_code must be supplied together")
	}
	if b.RatePolicyCode != "" {
		raw, err := strconv.ParseUint(b.RatePolicyCode, 10, 64)
		if err != nil {
			return fmt.Errorf("rate_policy_code must be canonical uint64 decimal text: %w", err)
		}
		if strconv.FormatUint(raw, 10) != b.RatePolicyCode {
			return errors.New("rate_policy_code must be canonical uint64 decimal text")
		}
	}
	if b.RedisACLProfile != "" {
		if err := validateSegment(b.RedisACLProfile); err != nil {
			return fmt.Errorf("redis_acl_profile: %w", err)
		}
	}
	if b.Revision == 0 {
		return errors.New("revision must be non-zero")
	}
	return nil
}

func (b Binding) Digest() (string, error) {
	if err := b.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(b)
	if err != nil {
		return "", err
	}
	return digestBytes(payload), nil
}

// RedisKey is the mutable control-plane alias for the current binding at this
// tenant/service/route scope. Durable execution records must freeze Digest().
func (b Binding) RedisKey() (string, error) {
	if err := b.Validate(); err != nil {
		return "", err
	}
	parts := []string{b.OrgID, "fatline", "auth", "binding"}
	switch {
	case b.RouteID != "":
		parts = append(parts, "route", b.TenantID, b.ServiceID, b.RouteID)
	case b.ServiceID != "":
		parts = append(parts, "service", b.TenantID, b.ServiceID)
	default:
		parts = append(parts, "tenant", b.TenantID)
	}
	return strings.Join(parts, ":"), nil
}

func (b Binding) ArtifactRedisKey() (string, error) {
	digest, err := b.Digest()
	if err != nil {
		return "", err
	}
	return b.OrgID + ":fatline:auth:binding-artifact:" + strings.TrimPrefix(digest, "sha256:"), nil
}

func PolicyRedisKey(orgID, digest string) (string, error) {
	if err := validateSegment(orgID); err != nil {
		return "", err
	}
	if !validDigest(digest) {
		return "", errors.New("invalid policy digest")
	}
	return orgID + ":fatline:auth:policy:" + strings.TrimPrefix(digest, "sha256:"), nil
}

func RuntimeKey(orgID, family, tenantID string, identity ...string) (string, error) {
	parts := []string{orgID, "fatline", "runtime", family, tenantID}
	parts = append(parts, identity...)
	for _, part := range parts {
		if err := validateSegment(part); err != nil {
			return "", err
		}
	}
	return strings.Join(parts, ":"), nil
}

func digestBytes(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	raw := strings.TrimPrefix(value, "sha256:")
	if raw != strings.ToLower(raw) {
		return false
	}
	_, err := hex.DecodeString(raw)
	return err == nil
}

func validateSegment(value string) error {
	if value == "" {
		return errors.New("keyspace segment is empty")
	}
	if strings.ContainsAny(value, ":*?[]{}\\") || strings.IndexFunc(value, unicode.IsSpace) >= 0 {
		return errors.New("keyspace segment contains reserved Redis syntax or whitespace")
	}
	return nil
}
