package routes

import (
	"net/http"
	"strings"
	"testing"

	"github.com/xd-dash/logma/internal/pubsubmodel"
)

func simpleTestRouter(t *testing.T, store *fakePubSubResourceStore) http.Handler {
	t.Helper()
	t.Setenv("API_KEY", "test-api-key")
	api := &simplePubSubAPI{
		store: func() (pubSubResourceStore, error) { return store, nil },
		newID: func() (string, error) { return "generated-sub", nil },
	}
	return newSimplePubSubRouter(api)
}

func TestSimplePubSubSubscribeComposesResources(t *testing.T) {
	store := newFakePubSubResourceStore()
	router := simpleTestRouter(t, store)

	resp := requestResource(t, router, http.MethodPost, "/subscribe", `{"channel":"market:quotes","callbackURL":"https://example.invalid/hook"}`)
	if resp.Code != http.StatusCreated {
		t.Fatalf("POST /subscribe status=%d body=%s", resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), `"subscriptionID":"generated-sub"`) {
		t.Fatalf("POST /subscribe body=%s", resp.Body.String())
	}
	if _, ok := store.channels["market:quotes"]; !ok {
		t.Fatal("Channel was not composed")
	}
	callback, ok := store.callbacks["generated-sub-callback"]
	if !ok || callback.Webhook == nil || callback.Webhook.CallbackURL != "https://example.invalid/hook" {
		t.Fatalf("Callback=%+v", callback)
	}
	subscriber, ok := store.subscribers["generated-sub"]
	if !ok {
		t.Fatal("Subscriber was not composed")
	}
	if subscriber.Channel != "market:quotes" || len(subscriber.CallbackIDs) != 1 || subscriber.CallbackIDs[0] != "generated-sub-callback" {
		t.Fatalf("Subscriber=%+v", subscriber)
	}
}

func TestSimplePubSubSubscribeAcceptsExplicitIDs(t *testing.T) {
	store := newFakePubSubResourceStore()
	router := simpleTestRouter(t, store)

	resp := requestResource(t, router, http.MethodPost, "/subscribe", `{"id":"screen","callbackID":"screen-hook","channel":"market","callbackURL":"https://example.invalid/hook"}`)
	if resp.Code != http.StatusCreated {
		t.Fatalf("POST /subscribe status=%d body=%s", resp.Code, resp.Body.String())
	}
	if _, ok := store.subscribers["screen"]; !ok {
		t.Fatal("explicit Subscriber id was not preserved")
	}
	if _, ok := store.callbacks["screen-hook"]; !ok {
		t.Fatal("explicit Callback id was not preserved")
	}
}

func TestSimplePubSubSubscribeRejectsInvalidWebhookBeforePersistence(t *testing.T) {
	store := newFakePubSubResourceStore()
	router := simpleTestRouter(t, store)

	resp := requestResource(t, router, http.MethodPost, "/subscribe", `{"channel":"market","callbackURL":"not-a-url"}`)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("invalid webhook status=%d body=%s", resp.Code, resp.Body.String())
	}
	if len(store.channels) != 0 || len(store.callbacks) != 0 || len(store.subscribers) != 0 {
		t.Fatalf("invalid request persisted resources: channels=%v callbacks=%v subscribers=%v", store.channels, store.callbacks, store.subscribers)
	}
}

func TestSimplePubSubSubscribeSurfacesGraphConflict(t *testing.T) {
	store := newFakePubSubResourceStore()
	store.putSubErr = pubsubmodel.ErrMissingReference
	router := simpleTestRouter(t, store)

	resp := requestResource(t, router, http.MethodPost, "/subscribe", `{"channel":"market","callbackURL":"https://example.invalid/hook"}`)
	if resp.Code != http.StatusConflict {
		t.Fatalf("graph conflict status=%d body=%s", resp.Code, resp.Body.String())
	}
}

func TestSimplePubSubStateUsesOperatorVocabulary(t *testing.T) {
	store := newFakePubSubResourceStore()
	store.channels["market"] = pubsubmodel.Channel{Name: "market"}
	store.subscribers["screen"] = pubsubmodel.Subscriber{ID: "screen", Channel: "market", CallbackIDs: []string{"hook"}}
	fakePublishers[store] = map[string]pubsubmodel.Publisher{
		"stonks": {ID: "stonks", Channel: "market", Type: "stonks"},
	}
	fakeSubscriptionGroups[store] = map[string]pubsubmodel.SubscriptionGroup{
		"morning": {ID: "morning"},
	}
	fakePublisherGroups[store] = map[string]pubsubmodel.PublisherGroup{
		"feeds": {ID: "feeds"},
	}
	router := simpleTestRouter(t, store)

	resp := requestResource(t, router, http.MethodGet, "/state", "")
	if resp.Code != http.StatusOK {
		t.Fatalf("GET /state status=%d body=%s", resp.Code, resp.Body.String())
	}
	for _, want := range []string{`"channels":["market"]`, `"subscriptions":["screen"]`, `"publishers":["stonks"]`, `"groups":["morning"]`, `"publisherGroups":["feeds"]`} {
		if !strings.Contains(resp.Body.String(), want) {
			t.Fatalf("GET /state missing %s in %s", want, resp.Body.String())
		}
	}
}
