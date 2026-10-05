package keyspace

import "fmt"

// logmaLifecycleRequirements preserves the qualified lifecycle wire namespace.
// Lifecycle is best-effort signaling, independent of graph and stream authority.
func logmaLifecycleRequirements(scope Scope, access Access) (RedisRequirements, error) {
	if access == 0 || access&^(AccessPublish|AccessSubscribe) != 0 {
		return RedisRequirements{}, fmt.Errorf("logma lifecycle supports publish/subscribe access only")
	}
	family, err := NewFamily(scope, "logma", "lifecycle")
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
