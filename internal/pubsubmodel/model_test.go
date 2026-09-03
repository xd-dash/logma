package pubsubmodel

import (
	"encoding/json"
	"testing"
)

func TestChannelAllowsNoSubscriber(t *testing.T) {
	channel := Channel{Name: "dev:news:items"}
	if err := channel.Validate(); err != nil {
		t.Fatalf("channel should be valid without subscribers: %v", err)
	}
}

func TestSubscriberRequiresCallback(t *testing.T) {
	subscriber := Subscriber{ID: "subscriber-1", Channel: "dev:news:items"}
	if err := subscriber.Validate(); err == nil {
		t.Fatal("subscriber without callbacks should be invalid")
	}
	subscriber.CallbackIDs = []string{"callback-1"}
	if err := subscriber.Validate(); err != nil {
		t.Fatalf("subscriber with callback should be valid: %v", err)
	}
}

func TestTypedCallbacks(t *testing.T) {
	webhook := Callback{ID: "callback-webhook", Type: CallbackWebhook, Webhook: &WebhookCallback{CallbackURL: "https://example.test/hook"}}
	if err := webhook.Validate(); err != nil {
		t.Fatalf("webhook callback should be valid: %v", err)
	}
	lua := Callback{ID: "callback-lua", Type: CallbackLua, Lua: &LuaCallback{Name: "logma_on_news"}}
	if err := lua.Validate(); err != nil {
		t.Fatalf("lua callback should be valid: %v", err)
	}
}

func TestWebhookAllowsMultipleCallbackURLs(t *testing.T) {
	callback := Callback{
		ID:   "callback-webhook-many",
		Type: CallbackWebhook,
		Webhook: &WebhookCallback{CallbackURLs: []string{
			"https://example.test/one",
			" https://example.test/two ",
		}},
	}
	if err := callback.Validate(); err != nil {
		t.Fatalf("multi-URL webhook callback should be valid: %v", err)
	}
	urls := callback.Webhook.URLs()
	if len(urls) != 2 {
		t.Fatalf("expected 2 webhook targets, got %d", len(urls))
	}
	if urls[0] != "https://example.test/one" || urls[1] != "https://example.test/two" {
		t.Fatalf("unexpected normalized webhook targets: %#v", urls)
	}
}

func TestWebhookCombinesSingleAndMultipleURLForms(t *testing.T) {
	callback := Callback{
		ID:   "callback-webhook-compatible",
		Type: CallbackWebhook,
		Webhook: &WebhookCallback{
			CallbackURL:  "https://example.test/primary",
			CallbackURLs: []string{"https://example.test/secondary"},
		},
	}
	if err := callback.Validate(); err != nil {
		t.Fatalf("combined webhook URL forms should be valid: %v", err)
	}
	urls := callback.Webhook.URLs()
	if len(urls) != 2 || urls[0] != "https://example.test/primary" || urls[1] != "https://example.test/secondary" {
		t.Fatalf("unexpected combined webhook targets: %#v", urls)
	}
}

func TestWebhookRequiresAtLeastOneNonEmptyURL(t *testing.T) {
	callback := Callback{
		ID:      "callback-webhook-empty",
		Type:    CallbackWebhook,
		Webhook: &WebhookCallback{CallbackURL: " ", CallbackURLs: []string{"", "  "}},
	}
	if err := callback.Validate(); err == nil {
		t.Fatal("webhook without a non-empty callback URL should be invalid")
	}
}

func TestWebhookRequiresAbsoluteHTTPURL(t *testing.T) {
	for _, target := range []string{
		"relative/path",
		"example.test/hook",
		"ftp://example.test/hook",
		"https:///missing-host",
	} {
		callback := Callback{ID: "callback-bad-url", Type: CallbackWebhook, Webhook: &WebhookCallback{CallbackURL: target}}
		if err := callback.Validate(); err == nil {
			t.Fatalf("webhook target %q should be invalid", target)
		}
	}
}

func TestCallbackConfigurationMatchesType(t *testing.T) {
	callback := Callback{
		ID:      "callback-bad",
		Type:    CallbackWebhook,
		Webhook: &WebhookCallback{CallbackURL: "https://example.test/hook"},
		Lua:     &LuaCallback{Name: "unexpected"},
	}
	if err := callback.Validate(); err == nil {
		t.Fatal("mixed callback configuration should be invalid")
	}
}

func TestSubscriptionGroupAllowsEmptyMembership(t *testing.T) {
	group := SubscriptionGroup{ID: "group-1"}
	if err := group.Validate(); err != nil {
		t.Fatalf("empty subscription group should be valid: %v", err)
	}
	group.SubscriberIDs = []string{"subscriber-1", "subscriber-2"}
	if err := group.Validate(); err != nil {
		t.Fatalf("subscription group with members should be valid: %v", err)
	}
}

func TestSubscriptionGroupRejectsEmptySubscriberIdentity(t *testing.T) {
	group := SubscriptionGroup{ID: "group-1", SubscriberIDs: []string{"subscriber-1", " "}}
	if err := group.Validate(); err == nil {
		t.Fatal("subscription group with empty subscriber identity should be invalid")
	}
}

func TestPublisherAndServerlessEndpointAreIndependentResources(t *testing.T) {
	publisher := Publisher{ID: "news", Channel: "dev:news:items", Type: "xd-dash/news", Config: json.RawMessage(`{"source":"wire"}`)}
	if err := publisher.Validate(); err != nil {
		t.Fatalf("publisher should be valid: %v", err)
	}
	endpoint := ServerlessEndpoint{ID: "events", Type: "sse", Path: "/events"}
	if err := endpoint.Validate(); err != nil {
		t.Fatalf("serverless endpoint should be valid: %v", err)
	}
}

func TestPublisherRejectsInvalidOpaqueJSON(t *testing.T) {
	publisher := Publisher{ID: "news", Channel: "dev:news:items", Type: "xd-dash/news", Config: json.RawMessage(`{"broken":`)}
	if err := publisher.Validate(); err == nil {
		t.Fatal("publisher with invalid JSON config should be invalid")
	}
}
