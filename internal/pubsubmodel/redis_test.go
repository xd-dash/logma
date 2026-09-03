package pubsubmodel

import "testing"

func TestRedisKeysUseScopeFirstResourceGrammar(t *testing.T) {
	keys, err := NewRedisKeys(" dev:global ")
	if err != nil {
		t.Fatalf("new redis keys: %v", err)
	}

	cases := map[string]string{
		"channel":              keys.Channel("news"),
		"channel subscribers":  keys.ChannelSubscribers("news"),
		"channel publishers":   keys.ChannelPublishers("news"),
		"callback":             keys.Callback("webhook-1"),
		"callback subscribers": keys.CallbackSubscribers("webhook-1"),
		"callback urls":        keys.CallbackURLs("webhook-1"),
		"subscriber":           keys.Subscriber("subscriber-1"),
		"subscriber callbacks": keys.SubscriberCallbacks("subscriber-1"),
		"publisher":            keys.Publisher("news-publisher"),
		"group":                keys.SubscriptionGroup("observers"),
		"group subscribers":    keys.SubscriptionGroupSubscribers("observers"),
	}

	want := map[string]string{
		"channel":              "dev:global:logma:pubsub:channel:news",
		"channel subscribers":  "dev:global:logma:pubsub:channel:news:subscribers",
		"channel publishers":   "dev:global:logma:pubsub:channel:news:publishers",
		"callback":             "dev:global:logma:pubsub:callback:webhook-1",
		"callback subscribers": "dev:global:logma:pubsub:callback:webhook-1:subscribers",
		"callback urls":        "dev:global:logma:pubsub:callback:webhook-1:urls",
		"subscriber":           "dev:global:logma:pubsub:subscriber:subscriber-1",
		"subscriber callbacks": "dev:global:logma:pubsub:subscriber:subscriber-1:callbacks",
		"publisher":            "dev:global:logma:pubsub:publisher:news-publisher",
		"group":                "dev:global:logma:pubsub:group:observers",
		"group subscribers":    "dev:global:logma:pubsub:group:observers:subscribers",
	}

	for name, got := range cases {
		if got != want[name] {
			t.Fatalf("%s key = %q, want %q", name, got, want[name])
		}
	}
}

func TestRedisKeysRequireExplicitScope(t *testing.T) {
	for _, scope := range []string{"", " ", ":"} {
		if _, err := NewRedisKeys(scope); err == nil {
			t.Fatalf("scope %q should be rejected", scope)
		}
	}
}

func TestUniqueTrimmedPreservesFirstSeenOrder(t *testing.T) {
	got := uniqueTrimmed([]string{" a ", "b", "a", "", " b ", "c"})
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	}
}
