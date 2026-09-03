package keyspace

import "fmt"

// Access is semantic resource authority. Provider implementations translate it
// into their concrete storage/transport permissions.
type Access uint8

const (
	AccessRead Access = 1 << iota
	AccessWrite
	AccessInvoke
	AccessPublish
	AccessSubscribe
)

// Capability identifies a domain/resource family rather than provider syntax.
type Capability string

const (
	CapabilityLogmaPubSubGraph     Capability = "logma.pubsub.graph"
	CapabilityLogmaPubSubTransport Capability = "logma.pubsub.transport"
)

// Grant is one semantic capability assignment.
type Grant struct {
	Capability Capability
	Access     Access
}

// RedisRequirements is the provider-specific material authority required to
// realize semantic grants against Redis.
type RedisRequirements struct {
	KeyPatterns     []string
	ChannelPatterns []string
	Commands        []string
}

// CompileRedisRequirements maps semantic v2 grants to concrete Redis ACL
// requirements. The mapping is intentionally explicit and fails closed for
// capabilities/access combinations this provider does not know how to realize.
func CompileRedisRequirements(scope Scope, grants ...Grant) (RedisRequirements, error) {
	if _, err := ParseScope(string(scope)); err != nil {
		return RedisRequirements{}, err
	}
	var out RedisRequirements
	for _, grant := range grants {
		var req RedisRequirements
		var err error
		switch grant.Capability {
		case CapabilityLogmaPubSubGraph:
			req, err = logmaPubSubGraphRequirements(scope, grant.Access)
		case CapabilityLogmaPubSubTransport:
			req, err = logmaPubSubTransportRequirements(scope, grant.Access)
		default:
			return RedisRequirements{}, fmt.Errorf("unsupported Redis capability %q", grant.Capability)
		}
		if err != nil {
			return RedisRequirements{}, err
		}
		out.KeyPatterns = append(out.KeyPatterns, req.KeyPatterns...)
		out.ChannelPatterns = append(out.ChannelPatterns, req.ChannelPatterns...)
		out.Commands = append(out.Commands, req.Commands...)
	}
	out.KeyPatterns = unique(out.KeyPatterns)
	out.ChannelPatterns = unique(out.ChannelPatterns)
	out.Commands = unique(out.Commands)
	return out, nil
}

func baseRedisCommands() []string {
	return []string{"ping", "hello", "client"}
}

func logmaPubSubGraphRequirements(scope Scope, access Access) (RedisRequirements, error) {
	if access == 0 || access&^(AccessRead|AccessWrite) != 0 {
		return RedisRequirements{}, fmt.Errorf("logma Pub/Sub graph supports read/write access only")
	}
	family, err := NewFamily(scope, "logma", "pubsub")
	if err != nil {
		return RedisRequirements{}, err
	}
	req := RedisRequirements{
		KeyPatterns: []string{family.KeyPattern()},
		Commands:    baseRedisCommands(),
	}

	if access&AccessRead != 0 {
		req.Commands = append(req.Commands,
			"hget", "hgetall", "hexists",
			"smembers", "scard", "exists",
		)
	}
	if access&AccessWrite != 0 {
		// Mutation authority must be self-sufficient. Subscriber/publisher
		// reconciliation and guarded deletes read existing graph state as part
		// of optimistic WATCH transactions. These reads are provider
		// prerequisites, not an independent semantic Read grant.
		req.Commands = append(req.Commands,
			"hset", "hdel",
			"sadd", "srem",
			"del",
			"watch", "unwatch", "multi", "exec", "discard",
			"exists", "hget", "smembers", "scard",
		)
	}
	return req, nil
}

func logmaPubSubTransportRequirements(scope Scope, access Access) (RedisRequirements, error) {
	if access == 0 || access&^(AccessPublish|AccessSubscribe) != 0 {
		return RedisRequirements{}, fmt.Errorf("logma Pub/Sub transport supports publish/subscribe access only")
	}
	family, err := LogmaPubSubTransportFamily(scope)
	if err != nil {
		return RedisRequirements{}, err
	}
	req := RedisRequirements{
		ChannelPatterns: []string{family.ChannelPattern()},
		Commands:        baseRedisCommands(),
	}
	if access&AccessPublish != 0 {
		req.Commands = append(req.Commands, "publish")
	}
	if access&AccessSubscribe != 0 {
		req.Commands = append(req.Commands, "subscribe", "unsubscribe")
	}
	return req, nil
}
