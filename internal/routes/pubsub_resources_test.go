package routes

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xd-dash/logma/internal/pubsubmodel"
)

type fakePubSubResourceStore struct {
	channels    map[string]pubsubmodel.Channel
	callbacks   map[string]pubsubmodel.Callback
	subscribers map[string]pubsubmodel.Subscriber
	putSubErr   error
}

func newFakePubSubResourceStore() *fakePubSubResourceStore {
	return &fakePubSubResourceStore{
		channels:    make(map[string]pubsubmodel.Channel),
		callbacks:   make(map[string]pubsubmodel.Callback),
		subscribers: make(map[string]pubsubmodel.Subscriber),
	}
}

func (s *fakePubSubResourceStore) PutChannel(_ context.Context, resource pubsubmodel.Channel) error {
	s.channels[resource.Name] = resource
	return nil
}

func (s *fakePubSubResourceStore) GetChannel(_ context.Context, name string) (pubsubmodel.Channel, error) {
	resource, ok := s.channels[name]
	if !ok {
		return pubsubmodel.Channel{}, pubsubmodel.ErrNotFound
	}
	return resource, nil
}

func (s *fakePubSubResourceStore) PutCallback(_ context.Context, resource pubsubmodel.Callback) error {
	s.callbacks[resource.ID] = resource
	return nil
}

func (s *fakePubSubResourceStore) GetCallback(_ context.Context, id string) (pubsubmodel.Callback, error) {
	resource, ok := s.callbacks[id]
	if !ok {
		return pubsubmodel.Callback{}, pubsubmodel.ErrNotFound
	}
	return resource, nil
}

func (s *fakePubSubResourceStore) PutSubscriber(_ context.Context, resource pubsubmodel.Subscriber) error {
	if s.putSubErr != nil {
		return s.putSubErr
	}
	s.subscribers[resource.ID] = resource
	return nil
}

func (s *fakePubSubResourceStore) GetSubscriber(_ context.Context, id string) (pubsubmodel.Subscriber, error) {
	resource, ok := s.subscribers[id]
	if !ok {
		return pubsubmodel.Subscriber{}, pubsubmodel.ErrNotFound
	}
	return resource, nil
}

func resourceTestRouter(t *testing.T, store *fakePubSubResourceStore) http.Handler {
	t.Helper()
	t.Setenv("API_KEY", "test-api-key")
	api := &pubSubResourceAPI{store: func() (pubSubResourceStore, error) { return store, nil }}
	return newPubSubResourceRouter(api)
}

func requestResource(t *testing.T, router http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set(apiKeyHeader, "test-api-key")
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	return resp
}

func TestPubSubResourceAPIChannelCallbackSubscriberRoundTrip(t *testing.T) {
	store := newFakePubSubResourceStore()
	router := resourceTestRouter(t, store)

	resp := requestResource(t, router, http.MethodPost, "/pubsub/channels", `{"name":"events"}`)
	if resp.Code != http.StatusCreated {
		t.Fatalf("POST Channel status = %d, body = %s", resp.Code, resp.Body.String())
	}
	resp = requestResource(t, router, http.MethodGet, "/pubsub/channels/events", "")
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), `"name":"events"`) {
		t.Fatalf("GET Channel status = %d, body = %s", resp.Code, resp.Body.String())
	}

	resp = requestResource(t, router, http.MethodPost, "/pubsub/callbacks", `{"id":"hook","type":"webhook","webhook":{"callbackURLs":["https://one.example/hook","https://two.example/hook"]}}`)
	if resp.Code != http.StatusCreated {
		t.Fatalf("POST Callback status = %d, body = %s", resp.Code, resp.Body.String())
	}
	resp = requestResource(t, router, http.MethodGet, "/pubsub/callbacks/hook", "")
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), `"id":"hook"`) {
		t.Fatalf("GET Callback status = %d, body = %s", resp.Code, resp.Body.String())
	}

	resp = requestResource(t, router, http.MethodPost, "/pubsub/subscribers", `{"id":"sub-a","channel":"events","callbackIDs":["hook"]}`)
	if resp.Code != http.StatusCreated {
		t.Fatalf("POST Subscriber status = %d, body = %s", resp.Code, resp.Body.String())
	}
	resp = requestResource(t, router, http.MethodGet, "/pubsub/subscribers/sub-a", "")
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), `"callbackIDs":["hook"]`) {
		t.Fatalf("GET Subscriber status = %d, body = %s", resp.Code, resp.Body.String())
	}
}

func TestPubSubResourceAPIRejectsInvalidAndMissingReferences(t *testing.T) {
	store := newFakePubSubResourceStore()
	router := resourceTestRouter(t, store)

	resp := requestResource(t, router, http.MethodPost, "/pubsub/callbacks", `{"id":"bad","type":"webhook"}`)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("invalid Callback status = %d, body = %s", resp.Code, resp.Body.String())
	}

	store.putSubErr = errors.New("subscriber references missing channel or callback")
	resp = requestResource(t, router, http.MethodPost, "/pubsub/subscribers", `{"id":"sub-a","channel":"missing","callbackIDs":["missing"]}`)
	if resp.Code != http.StatusConflict {
		t.Fatalf("missing references status = %d, body = %s", resp.Code, resp.Body.String())
	}

	resp = requestResource(t, router, http.MethodGet, "/pubsub/channels/missing", "")
	if resp.Code != http.StatusNotFound {
		t.Fatalf("missing Channel status = %d, body = %s", resp.Code, resp.Body.String())
	}
}

func TestPubSubResourceAPIRequiresExplicitFatlineScope(t *testing.T) {
	t.Setenv("API_KEY", "test-api-key")
	t.Setenv("FATLINE_SCOPE", "")
	router := NewPubSubResourceRouter()

	resp := requestResource(t, router, http.MethodPost, "/pubsub/channels", `{"name":"events"}`)
	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing FATLINE_SCOPE status = %d, body = %s", resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), "FATLINE scope is required") {
		t.Fatalf("missing FATLINE_SCOPE body = %q", resp.Body.String())
	}
}
