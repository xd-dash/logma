package pubsubmodel

import "testing"

func TestChannelAllowsNoSubscriber(t *testing.T) {
	channel := Channel{Name: "dev:news:items"}
	if err := channel.Validate(); err != nil {
		t.Fatalf("channel should be valid without subscribers: %v", err)
	}
}

func TestSubscriberRequiresCallback(t *testing.T) {
	subscriber := Subscriber{
		ID:      "subscriber-1",
		Channel: "dev:news:items",
	}
	if err := subscriber.Validate(); err == nil {
		t.Fatal("subscriber without callbacks should be invalid")
	}

	subscriber.CallbackIDs = []string{"callback-1"}
	if err := subscriber.Validate(); err != nil {
		t.Fatalf("subscriber with callback should be valid: %v", err)
	}
}

func TestTypedCallbacks(t *testing.T) {
	webhook := Callback{
		ID:   "callback-webhook",
		Type: CallbackWebhook,
		Webhook: &WebhookCallback{
			CallbackURL: "https://example.test/hook",
		},
	}
	if err := webhook.Validate(); err != nil {
		t.Fatalf("webhook callback should be valid: %v", err)
	}

	lua := Callback{
		ID:   "callback-lua",
		Type: CallbackLua,
		Lua: &LuaCallback{
			Name: "logma_on_news",
		},
	}
	if err := lua.Validate(); err != nil {
		t.Fatalf("lua callback should be valid: %v", err)
	}
}

func TestCallbackConfigurationMatchesType(t *testing.T) {
	callback := Callback{
		ID:   "callback-bad",
		Type: CallbackWebhook,
		Webhook: &WebhookCallback{
			CallbackURL: "https://example.test/hook",
		},
		Lua: &LuaCallback{Name: "unexpected"},
	}
	if err := callback.Validate(); err == nil {
		t.Fatal("mixed callback configuration should be invalid")
	}
}

func TestPublisherAndServerlessEndpointAreIndependentResources(t *testing.T) {
	publisher := Publisher{
		ID:      "news",
		Channel: "dev:news:items",
		Type:    "xd-dash/news",
	}
	if err := publisher.Validate(); err != nil {
		t.Fatalf("publisher should be valid: %v", err)
	}

	endpoint := ServerlessEndpoint{
		ID:   "events",
		Type: "sse",
		Path: "/events",
	}
	if err := endpoint.Validate(); err != nil {
		t.Fatalf("serverless endpoint should be valid: %v", err)
	}
}
