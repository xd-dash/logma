package pubsubmodel

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xd-dash/logma/serverless/keyspace"
)

// ResourceKeysV2 is the canonical Redis address mapper for the Logma Pub/Sub
// graph on the Fatline v2 branch. Production RedisStore operations use this
// mapper directly; legacy persisted naming remains isolated to compatibility
// lines/providers rather than hidden fallback logic here.
type ResourceKeysV2 struct {
	scope keyspace.Scope
}

func NewResourceKeysV2(scope string) (ResourceKeysV2, error) {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return ResourceKeysV2{}, errors.New("FATLINE scope is required")
	}
	parsed, err := keyspace.ParseScope(scope)
	if err != nil {
		return ResourceKeysV2{}, err
	}
	return ResourceKeysV2{scope: parsed}, nil
}

func (k ResourceKeysV2) family(parts ...string) keyspace.Family {
	family, err := keyspace.NewFamily(k.scope, parts...)
	if err != nil {
		panic(err)
	}
	return family
}

func (k ResourceKeysV2) resource(kind, id string) string {
	resource, err := k.family("logma", "pubsub", kind).Resource(id)
	if err != nil {
		panic(err)
	}
	return resource.Key()
}

func (k ResourceKeysV2) child(kind, id, child string) string {
	resource, err := k.family("logma", "pubsub", kind).Resource(id)
	if err != nil {
		panic(err)
	}
	resource, err = resource.Child(child)
	if err != nil {
		panic(err)
	}
	return resource.Key()
}

func (k ResourceKeysV2) registry(kind string) string {
	return k.family("logma", "pubsub", "registry", kind).Key()
}

func (k ResourceKeysV2) GraphKeyPattern() string { return k.family("logma", "pubsub").KeyPattern() }

func (k ResourceKeysV2) Channels() string           { return k.registry("channels") }
func (k ResourceKeysV2) Callbacks() string          { return k.registry("callbacks") }
func (k ResourceKeysV2) Subscribers() string        { return k.registry("subscribers") }
func (k ResourceKeysV2) Publishers() string         { return k.registry("publishers") }
func (k ResourceKeysV2) SubscriptionGroups() string { return k.registry("subscription-groups") }
func (k ResourceKeysV2) PublisherGroups() string    { return k.registry("publisher-groups") }

func (k ResourceKeysV2) Channel(name string) string { return k.resource("channel", strings.TrimSpace(name)) }
func (k ResourceKeysV2) ChannelSubscribers(name string) string {
	return k.child("channel", strings.TrimSpace(name), "subscribers")
}
func (k ResourceKeysV2) ChannelPublishers(name string) string {
	return k.child("channel", strings.TrimSpace(name), "publishers")
}
func (k ResourceKeysV2) Callback(id string) string { return k.resource("callback", strings.TrimSpace(id)) }
func (k ResourceKeysV2) CallbackSubscribers(id string) string {
	return k.child("callback", strings.TrimSpace(id), "subscribers")
}
func (k ResourceKeysV2) CallbackURLs(id string) string {
	return k.child("callback", strings.TrimSpace(id), "urls")
}
func (k ResourceKeysV2) Subscriber(id string) string { return k.resource("subscriber", strings.TrimSpace(id)) }
func (k ResourceKeysV2) SubscriberCallbacks(id string) string {
	return k.child("subscriber", strings.TrimSpace(id), "callbacks")
}
func (k ResourceKeysV2) SubscriberGroups(id string) string {
	return k.child("subscriber", strings.TrimSpace(id), "subscription-groups")
}
func (k ResourceKeysV2) Publisher(id string) string { return k.resource("publisher", strings.TrimSpace(id)) }
func (k ResourceKeysV2) PublisherGroupsForPublisher(id string) string {
	return k.child("publisher", strings.TrimSpace(id), "publisher-groups")
}
func (k ResourceKeysV2) SubscriptionGroup(id string) string {
	return k.resource("subscription-group", strings.TrimSpace(id))
}
func (k ResourceKeysV2) SubscriptionGroupSubscribers(id string) string {
	return k.child("subscription-group", strings.TrimSpace(id), "subscribers")
}
func (k ResourceKeysV2) PublisherGroup(id string) string {
	return k.resource("publisher-group", strings.TrimSpace(id))
}
func (k ResourceKeysV2) PublisherGroupPublishers(id string) string {
	return k.child("publisher-group", strings.TrimSpace(id), "publishers")
}

func (k ResourceKeysV2) String() string { return fmt.Sprintf("ResourceKeysV2(scope=%s)", k.scope) }
