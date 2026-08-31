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
	if strings.ContainsAny(value, ":*?[]{} 	\r\n") {
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
		// Callers already need a usable Redis namespace. Keep the fallback bounded
		// rather than manufacturing a wildcard-capable ACL prefix.
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

// KeyPattern is the full read/write ACL pattern for this scope.
func (s Scope) KeyPattern() string { return "~" + clean(string(s)) + ":*" }

// ReadPattern and WritePattern use Redis 7+ directional key permissions.
func (s Scope) ReadPattern() string  { return "%R~" + clean(string(s)) + ":*" }
func (s Scope) WritePattern() string { return "%W~" + clean(string(s)) + ":*" }

// ChannelPattern is the Pub/Sub ACL pattern for this scope.
func (s Scope) ChannelPattern() string { return "&" + clean(string(s)) + ":*" }

// Worker describes a least-privilege Redis worker inside one Fatline scope.
// Command permissions remain deployment policy; this type owns only resource patterns.
type Worker struct {
	Scope      Scope
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
