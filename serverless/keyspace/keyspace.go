// Package keyspace defines Fatline scope, resource-address, and Redis ACL contracts.
package keyspace

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// Scope is the first Redis name segment and the primary ACL security boundary.
// A scope is intentionally opaque: deployment/tenant metadata belongs elsewhere.
type Scope string

func ParseScope(value string) (Scope, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("fatline scope is empty")
	}
	if strings.ContainsAny(value, ":*?[]{} \t\r\n") {
		return "", fmt.Errorf("fatline scope %q contains reserved Redis pattern characters", value)
	}
	return Scope(value), nil
}

// FromEnv is retained for compatibility with pre-v2 callers. New v2 resource
// and security code must parse scope explicitly and fail closed rather than
// accepting the synthetic "unknown" fallback used here.
func FromEnv(fallback string) Scope {
	value := strings.TrimSpace(os.Getenv("FATLINE_SCOPE"))
	if value == "" {
		value = strings.TrimSpace(fallback)
	}
	scope, err := ParseScope(value)
	if err != nil {
		return Scope("unknown")
	}
	return scope
}

// clean belongs to the legacy Name/Profile representation. It is deliberately
// lossy and must not be used to construct canonical v2 resource identities.
func clean(part string) string {
	part = strings.TrimSpace(part)
	part = strings.Trim(part, ":")
	part = strings.ReplaceAll(part, " ", "_")
	if part == "" {
		return "unknown"
	}
	return part
}

func validCapability(part string) bool {
	part = strings.TrimSpace(part)
	return part != "" && !strings.ContainsAny(part, ":*?[]{} \t\r\n")
}

func validCommand(command string) bool {
	command = strings.TrimSpace(command)
	return command != "" && command == strings.ToLower(command) && !strings.ContainsAny(command, "+-@~&%:*?[]{} \t\r\n")
}

// Name returns the legacy <scope>:<subsystem>:<resource...> representation.
// Canonical v2 identities use Family/Resource so opaque identity segments are
// encoded injectively instead of normalized by clean.
func (s Scope) Name(subsystem string, resource ...string) string {
	parts := []string{clean(string(s)), clean(subsystem)}
	for _, part := range resource {
		if strings.TrimSpace(part) != "" {
			parts = append(parts, clean(part))
		}
	}
	return strings.Join(parts, ":")
}

// Prefix scopes an already-structured legacy logical resource.
func (s Scope) Prefix(resource string) string {
	return clean(string(s)) + ":" + strings.TrimLeft(strings.TrimSpace(resource), ":")
}

func (s Scope) KeyPattern() string     { return "~" + clean(string(s)) + ":*" }
func (s Scope) ReadPattern() string    { return "%R~" + clean(string(s)) + ":*" }
func (s Scope) WritePattern() string   { return "%W~" + clean(string(s)) + ":*" }
func (s Scope) ChannelPattern() string { return "&" + clean(string(s)) + ":*" }

// Worker and Profile are retained provider-compatibility primitives. New v2
// capabilities should compile through semantic Grant values and
// CompileRedisRequirements rather than adding broader hand-authored profiles.
type Worker struct {
	Scope     Scope
	Subsystem string
}

func (w Worker) prefix() string {
	if strings.TrimSpace(w.Subsystem) == "" {
		return clean(string(w.Scope)) + ":*"
	}
	return w.Scope.Name(w.Subsystem) + ":*"
}
func (w Worker) KeyPattern() string     { return "~" + w.prefix() }
func (w Worker) ReadPattern() string    { return "%R~" + w.prefix() }
func (w Worker) WritePattern() string   { return "%W~" + w.prefix() }
func (w Worker) ChannelPattern() string { return "&" + w.prefix() }

type Profile struct {
	Scope             Scope
	KeySubsystems     []string
	ChannelSubsystems []string
	Commands          []string
	AllowGlobalRelay  bool
}

func (p Profile) Validate() error {
	if _, err := ParseScope(string(p.Scope)); err != nil {
		return err
	}
	for _, capability := range append(append([]string{}, p.KeySubsystems...), p.ChannelSubsystems...) {
		if !validCapability(capability) {
			return fmt.Errorf("invalid worker capability %q", capability)
		}
	}
	for _, command := range p.Commands {
		if !validCommand(command) {
			return fmt.Errorf("invalid Redis command capability %q", command)
		}
	}
	return nil
}

func unique(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (p Profile) ACLPatterns() ([]string, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	patterns := make([]string, 0, len(p.KeySubsystems)+len(p.ChannelSubsystems)+1)
	for _, subsystem := range unique(p.KeySubsystems) {
		patterns = append(patterns, Worker{Scope: p.Scope, Subsystem: subsystem}.KeyPattern())
	}
	for _, subsystem := range unique(p.ChannelSubsystems) {
		patterns = append(patterns, Worker{Scope: p.Scope, Subsystem: subsystem}.ChannelPattern())
	}
	if p.AllowGlobalRelay {
		patterns = append(patterns, "&global:*")
	}
	return patterns, nil
}

func (p Profile) ACLRules() ([]string, error) {
	patterns, err := p.ACLPatterns()
	if err != nil {
		return nil, err
	}
	rules := append([]string{}, patterns...)
	for _, command := range unique(p.Commands) {
		rules = append(rules, "+"+command)
	}
	return rules, nil
}

// NewsProfile is the qualified pre-v2 compatibility profile for the current
// News worker. Do not use it as the template for new v2 resource capability
// design; semantic capabilities should be added to the provider compiler.
func NewsProfile(scope Scope) Profile {
	return Profile{
		Scope:             scope,
		KeySubsystems:     []string{"logma", "ratelimiter"},
		ChannelSubsystems: []string{"news"},
		Commands: []string{
			"ping", "hello", "client",
			"multi", "exec",
			"hset", "hget", "hdel", "hexists", "expire",
			"sadd", "srem", "smembers", "del",
			"fcall", "publish", "subscribe", "unsubscribe",
			"type", "time",
			"zadd", "zcard", "zrange", "zrangebyscore", "zrem", "zremrangebyscore",
			"incr", "pexpire",
		},
	}
}
