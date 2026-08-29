// Package acl defines Logma-owned Redis ACL profiles and tenant scopes.
//
// Redis ACL users are the security boundary. Logma profiles only compile
// deterministic Redis ACL rules; they do not grant FUNCTION LOAD to tenants.
package acl

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type Capability uint32

const (
	CapabilityData Capability = 1 << iota
	CapabilityPublish
	CapabilitySubscribe
	CapabilityFunctions
)

type TenantPolicy struct {
	Name         string
	Capabilities Capability
}

func (p TenantPolicy) Has(cap Capability) bool {
	return p.Capabilities&cap == cap
}

type Profile struct {
	Name       string
	Managed    bool
	AdminUser  string
	TenantBase TenantPolicy
}

var identifierRE = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func ValidateIdentifier(value string) error {
	if value == "" || len(value) > 64 || !identifierRE.MatchString(value) {
		return errors.New("identifier must be 1-64 ASCII letters, digits, '.', '_' or '-'")
	}
	return nil
}

func TenantUsername(tenant string) string {
	return "logma-tenant-" + tenant
}

func TenantKeyPrefix(tenant string) string {
	return "logma:tenant:" + tenant + ":"
}

func TenantKeyPattern(tenant string) string {
	return "%RW~" + TenantKeyPrefix(tenant) + "*"
}

func TenantChannelPrefix(tenant string) string {
	return "tenant:" + tenant + ":"
}

func TenantChannelPattern(tenant string) string {
	return "&" + TenantChannelPrefix(tenant) + "*"
}

func TenantFunctionPrefix(tenant string) string {
	return "logma_" + strings.ReplaceAll(tenant, ".", "_") + "__"
}

func TenantFunctionName(tenant, name string) (string, error) {
	if err := ValidateIdentifier(tenant); err != nil {
		return "", fmt.Errorf("tenant: %w", err)
	}
	if err := ValidateIdentifier(name); err != nil {
		return "", fmt.Errorf("function name: %w", err)
	}
	return TenantFunctionPrefix(tenant) + name, nil
}

func TenantLibraryName(tenant, name string) (string, error) {
	fn, err := TenantFunctionName(tenant, name)
	if err != nil {
		return "", err
	}
	return "lib_" + fn, nil
}

// DataCommands is deliberately explicit. Avoid broad +@read/+@write grants:
// Redis categories evolve and some database-wide commands are not constrained
// by key patterns.
var DataCommands = []string{
	"+get", "+set", "+setnx", "+getset", "+mget", "+mset", "+msetnx",
	"+del", "+unlink", "+exists", "+expire", "+expireat", "+pexpire",
	"+pexpireat", "+persist", "+ttl", "+pttl", "+type", "+touch",
	"+getdel", "+getex", "+append", "+strlen", "+getrange", "+setrange",
	"+incr", "+incrby", "+incrbyfloat", "+decr", "+decrby",
	"+hget", "+hset", "+hdel", "+hexists", "+hgetall", "+hincrby",
	"+hincrbyfloat", "+hkeys", "+hlen", "+hmget", "+hmset", "+hscan",
	"+sadd", "+srem", "+smembers", "+sismember", "+smismember", "+scard",
	"+spop", "+srandmember", "+sscan",
	"+zadd", "+zrem", "+zrange", "+zrangebyscore", "+zrevrange", "+zscore",
	"+zmscore", "+zcard", "+zcount", "+zincrby", "+zscan",
	"+lpush", "+rpush", "+lpop", "+rpop", "+llen", "+lrange", "+lset",
	"+ltrim", "+lindex", "+lrem",
	"+xadd", "+xack", "+xdel", "+xlen", "+xrange", "+xrevrange", "+xtrim",
	"+xread", "+xreadgroup", "+xgroup",
	"+multi", "+exec", "+discard", "+watch", "+unwatch",
}

func RulesForTenant(tenant, password string, policy TenantPolicy, reset bool) ([]string, error) {
	if err := ValidateIdentifier(tenant); err != nil {
		return nil, err
	}
	if password == "" {
		return nil, errors.New("password is required")
	}

	rules := make([]string, 0, 8+len(DataCommands))
	if reset {
		rules = append(rules, "reset")
	} else {
		rules = append(rules, "-@all", "resetkeys", "resetchannels", "clearselectors", "resetpass")
	}
	rules = append(rules, "on", ">"+password, "-@all", "resetkeys", "resetchannels", "+ping")

	if policy.Has(CapabilityData) {
		rules = append(rules, TenantKeyPattern(tenant))
		rules = append(rules, DataCommands...)
	}
	if policy.Has(CapabilityPublish) {
		rules = append(rules, TenantChannelPattern(tenant), "+publish")
	}
	if policy.Has(CapabilitySubscribe) {
		if !policy.Has(CapabilityPublish) {
			rules = append(rules, TenantChannelPattern(tenant))
		}
		rules = append(rules, "+subscribe", "+unsubscribe", "+psubscribe", "+punsubscribe")
	}
	if policy.Has(CapabilityFunctions) {
		rules = append(rules, "+fcall", "+fcall_ro")
	}

	// Deliberately absent: EVAL/EVALSHA, FUNCTION *, ACL *, KEYS, SCAN,
	// FLUSH*, SWAPDB, CONFIG, MODULE, MIGRATE, DEBUG and broad categories.
	return rules, nil
}

func PolicyByName(name string) (TenantPolicy, error) {
	switch strings.TrimSpace(name) {
	case "", "tenant":
		return TenantPolicy{
			Name: "tenant",
			Capabilities: CapabilityData |
				CapabilityPublish |
				CapabilitySubscribe,
		}, nil
	case "tenant-functions":
		return TenantPolicy{
			Name: "tenant-functions",
			Capabilities: CapabilityData |
				CapabilityPublish |
				CapabilitySubscribe |
				CapabilityFunctions,
		}, nil
	case "publisher":
		return TenantPolicy{
			Name:         "publisher",
			Capabilities: CapabilityPublish,
		}, nil
	case "subscriber":
		return TenantPolicy{
			Name:         "subscriber",
			Capabilities: CapabilitySubscribe,
		}, nil
	default:
		return TenantPolicy{}, fmt.Errorf("unknown tenant ACL profile %q", name)
	}
}
