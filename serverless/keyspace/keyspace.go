// Package keyspace defines the scope-first Redis naming contract used by Fatline workers.
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

// Name returns <scope>:<subsystem>:<resource...>.
func (s Scope) Name(subsystem string, resource ...string) string {
	parts := []string{clean(string(s)), clean(subsystem)}
	for _, part := range resource {
		if strings.TrimSpace(part) != "" {
			parts = append(parts, clean(part))
		}
	}
	return strings.Join(parts, ":")
}

// Prefix scopes an already-structured logical resource, e.g. news:item:sec.
func (s Scope) Prefix(resource string) string {
	return clean(string(s)) + ":" + strings.TrimLeft(strings.TrimSpace(resource), ":")
}

func (s Scope) KeyPattern() string     { return "~" + clean(string(s)) + ":*" }
func (s Scope) ReadPattern() string    { return "%R~" + clean(string(s)) + ":*" }
func (s Scope) WritePattern() string   { return "%W~" + clean(string(s)) + ":*" }
func (s Scope) ChannelPattern() string { return "&" + clean(string(s)) + ":*" }

// Worker describes one subsystem-only worker. Prefer Profile for applications
// that legitimately need several package-owned capabilities (for example News
// publication plus Logma runtime records plus ratelimiter lifecycle state).
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

// Profile is the deployable least-privilege capability set for a Fatline
// worker. Key, Pub/Sub and command capabilities are all material authority and
// therefore travel together. Bootstrap-only commands such as FUNCTION LOAD are
// deliberately excluded from normal runtime profiles.
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

// ACLPatterns returns resource patterns only. Call ACLRules when materializing
// a complete runtime ACL so command permissions cannot silently drift from the
// package-owned worker profile.
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

// ACLRules returns the resource patterns plus explicit +command grants needed
// by the runtime profile. It never grants command categories or admin/script
// authority implicitly.
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

// NewsProfile captures the complete steady-state capabilities required by the
// current News worker: News publication, Logma invocation/runtime leases and
// ratelimiter lifecycle Functions. Redis ACLs enforce commands executed from
// inside FCALL against the caller, so the underlying Function commands are
// listed explicitly as well as FCALL itself.
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
