package routes

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/xd-dash/logma/internal/pubsubmodel"
)

func (s *fakePubSubResourceStore) CreateWebhookSubscription(_ context.Context, channel pubsubmodel.Channel, callback pubsubmodel.Callback, subscriber pubsubmodel.Subscriber) error {
	if s.putSubErr != nil {
		return s.putSubErr
	}
	channel.Name = strings.TrimSpace(channel.Name)
	callback.ID = strings.TrimSpace(callback.ID)
	subscriber.ID = strings.TrimSpace(subscriber.ID)
	if _, exists := s.callbacks[callback.ID]; exists {
		return pubsubmodel.ErrAlreadyExists
	}
	if _, exists := s.subscribers[subscriber.ID]; exists {
		return pubsubmodel.ErrAlreadyExists
	}
	s.channels[channel.Name] = channel
	s.callbacks[callback.ID] = callback
	s.subscribers[subscriber.ID] = subscriber
	return nil
}

func TestSimplePubSubSubscribeDoesNotOverwriteExplicitCallback(t *testing.T) {
	store := newFakePubSubResourceStore()
	store.callbacks["shared-hook"] = pubsubmodel.Callback{
		ID:      "shared-hook",
		Type:    pubsubmodel.CallbackWebhook,
		Webhook: &pubsubmodel.WebhookCallback{CallbackURL: "https://existing.example/hook"},
	}
	router := simpleTestRouter(t, store)
	resp := requestResource(t, router, http.MethodPost, "/subscribe", `{"id":"sub-new","callbackID":"shared-hook","channel":"new-channel","callbackURL":"https://replacement.example/hook"}`)
	if resp.Code != http.StatusConflict {
		t.Fatalf("callback collision status=%d body=%s", resp.Code, resp.Body.String())
	}
	if _, exists := store.channels["new-channel"]; exists {
		t.Fatal("failed atomic subscribe left a new Channel behind")
	}
	if _, exists := store.subscribers["sub-new"]; exists {
		t.Fatal("failed atomic subscribe left a Subscriber behind")
	}
	if got := store.callbacks["shared-hook"].Webhook.CallbackURL; got != "https://existing.example/hook" {
		t.Fatalf("existing Callback was overwritten: %q", got)
	}
}

func TestSimplePubSubSubscribeDoesNotPartiallyMutateOnSubscriberConflict(t *testing.T) {
	store := newFakePubSubResourceStore()
	store.subscribers["sub-existing"] = pubsubmodel.Subscriber{ID: "sub-existing", Channel: "old", CallbackIDs: []string{"old-hook"}}
	router := simpleTestRouter(t, store)
	resp := requestResource(t, router, http.MethodPost, "/subscribe", `{"id":"sub-existing","channel":"new-channel","callbackURL":"https://new.example/hook"}`)
	if resp.Code != http.StatusConflict {
		t.Fatalf("subscriber collision status=%d body=%s", resp.Code, resp.Body.String())
	}
	if _, exists := store.channels["new-channel"]; exists {
		t.Fatal("failed atomic subscribe left a new Channel behind")
	}
	if _, exists := store.callbacks["sub-existing-callback"]; exists {
		t.Fatal("failed atomic subscribe left a new Callback behind")
	}
	if got := store.subscribers["sub-existing"].Channel; got != "old" {
		t.Fatalf("existing Subscriber changed: channel=%q", got)
	}
}

func TestSimpleGroupOperationClassifiesControllerNotFoundAsMissing(t *testing.T) {
	store := newFakePubSubResourceStore()
	fakeSubscriptionGroups[store] = map[string]pubsubmodel.SubscriptionGroup{
		"morning": {ID: "morning", SubscriberIDs: []string{"gone"}},
	}
	controller := &fakeSimpleSubscriptionController{fail: map[string]error{"gone": pubsubmodel.ErrNotFound}}
	router := simpleTestRouterWithController(t, store, controller)
	resp := requestResource(t, router, http.MethodPost, "/groups/morning/activate", "")
	if resp.Code != http.StatusOK {
		t.Fatalf("activate status=%d body=%s", resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), `"missing":["gone"]`) {
		t.Fatalf("activate response did not classify missing member: %s", resp.Body.String())
	}
	if strings.Contains(resp.Body.String(), `"failed":["gone"]`) {
		t.Fatalf("missing member was misclassified as failed: %s", resp.Body.String())
	}
}
