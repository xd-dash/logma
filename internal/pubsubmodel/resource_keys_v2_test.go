package pubsubmodel

import "testing"

func TestResourceKeysV2MapPubSubGraphToCanonicalFamilies(t *testing.T) {
	keys, err := NewResourceKeysV2("dev-safe")
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		keys.Channel("dev:global:events"):            "dev-safe:logma:pubsub:channel:dev%3Aglobal%3Aevents",
		keys.ChannelSubscribers("dev:global:events"): "dev-safe:logma:pubsub:channel:dev%3Aglobal%3Aevents:subscribers",
		keys.ChannelPublishers("dev:global:events"):  "dev-safe:logma:pubsub:channel:dev%3Aglobal%3Aevents:publishers",
		keys.Callback("callback 1"):                  "dev-safe:logma:pubsub:callback:callback%201",
		keys.CallbackURLs("callback 1"):              "dev-safe:logma:pubsub:callback:callback%201:urls",
		keys.CallbackSubscribers("callback 1"):       "dev-safe:logma:pubsub:callback:callback%201:subscribers",
		keys.Subscriber("subscriber:1"):              "dev-safe:logma:pubsub:subscriber:subscriber%3A1",
		keys.SubscriberCallbacks("subscriber:1"):     "dev-safe:logma:pubsub:subscriber:subscriber%3A1:callbacks",
		keys.Publisher("publisher:1"):                "dev-safe:logma:pubsub:publisher:publisher%3A1",
		keys.SubscriptionGroup("group:1"):            "dev-safe:logma:pubsub:subscription-group:group%3A1",
		keys.SubscriptionGroupSubscribers("group:1"): "dev-safe:logma:pubsub:subscription-group:group%3A1:subscribers",
		keys.Channels():                              "dev-safe:logma:pubsub:registry:channels",
		keys.Callbacks():                             "dev-safe:logma:pubsub:registry:callbacks",
		keys.Subscribers():                           "dev-safe:logma:pubsub:registry:subscribers",
	}
	for got, want := range cases {
		if got != want {
			t.Fatalf("key=%q want %q", got, want)
		}
	}
	if got, want := keys.GraphKeyPattern(), "~dev-safe:logma:pubsub:*"; got != want {
		t.Fatalf("GraphKeyPattern=%q want %q", got, want)
	}
}

func TestResourceKeysV2SeparatesOpaqueIdentityFromStructuralChildren(t *testing.T) {
	keys, _ := NewResourceKeysV2("dev-safe")
	identityLookingLikeChild := keys.Channel("foo:subscribers")
	actualChild := keys.ChannelSubscribers("foo")
	if identityLookingLikeChild == actualChild {
		t.Fatalf("identity key collided with relationship key: %q", actualChild)
	}
}

func TestResourceKeysV2RejectsInvalidScope(t *testing.T) {
	for _, scope := range []string{"", "dev:*", "dev scope", "dev:scope"} {
		if _, err := NewResourceKeysV2(scope); err == nil {
			t.Fatalf("NewResourceKeysV2(%q) succeeded", scope)
		}
	}
}
