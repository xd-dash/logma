package routes

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/xd-dash/logma/internal/pubsubmodel"
)

var fakePublishers = map[*fakePubSubResourceStore]map[string]pubsubmodel.Publisher{}
var fakeSubscriptionGroups = map[*fakePubSubResourceStore]map[string]pubsubmodel.SubscriptionGroup{}
var fakePublisherGroups = map[*fakePubSubResourceStore]map[string]pubsubmodel.PublisherGroup{}

func sortedKeys[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func (s *fakePubSubResourceStore) ChannelIDs(context.Context) ([]string, error) {
	return sortedKeys(s.channels), nil
}

func (s *fakePubSubResourceStore) CallbackIDs(context.Context) ([]string, error) {
	return sortedKeys(s.callbacks), nil
}

func (s *fakePubSubResourceStore) SubscriberIDs(context.Context) ([]string, error) {
	return sortedKeys(s.subscribers), nil
}

func (s *fakePubSubResourceStore) PublisherIDs(context.Context) ([]string, error) {
	return sortedKeys(fakePublishers[s]), nil
}

func (s *fakePubSubResourceStore) SubscriptionGroupIDs(context.Context) ([]string, error) {
	return sortedKeys(fakeSubscriptionGroups[s]), nil
}

func (s *fakePubSubResourceStore) PublisherGroupIDs(context.Context) ([]string, error) {
	return sortedKeys(fakePublisherGroups[s]), nil
}

func (s *fakePubSubResourceStore) PutPublisher(_ context.Context, resource pubsubmodel.Publisher) error {
	if fakePublishers[s] == nil {
		fakePublishers[s] = map[string]pubsubmodel.Publisher{}
	}
	resource.ID = strings.TrimSpace(resource.ID)
	fakePublishers[s][resource.ID] = resource
	return nil
}

func (s *fakePubSubResourceStore) GetPublisher(_ context.Context, id string) (pubsubmodel.Publisher, error) {
	resource, ok := fakePublishers[s][strings.TrimSpace(id)]
	if !ok {
		return pubsubmodel.Publisher{}, pubsubmodel.ErrNotFound
	}
	return resource, nil
}

func (s *fakePubSubResourceStore) DeletePublisher(_ context.Context, id string) error {
	delete(fakePublishers[s], id)
	return nil
}

func (s *fakePubSubResourceStore) PutSubscriptionGroup(_ context.Context, resource pubsubmodel.SubscriptionGroup) error {
	if fakeSubscriptionGroups[s] == nil {
		fakeSubscriptionGroups[s] = map[string]pubsubmodel.SubscriptionGroup{}
	}
	resource.ID = strings.TrimSpace(resource.ID)
	fakeSubscriptionGroups[s][resource.ID] = resource
	return nil
}

func (s *fakePubSubResourceStore) GetSubscriptionGroup(_ context.Context, id string) (pubsubmodel.SubscriptionGroup, error) {
	resource, ok := fakeSubscriptionGroups[s][strings.TrimSpace(id)]
	if !ok {
		return pubsubmodel.SubscriptionGroup{}, pubsubmodel.ErrNotFound
	}
	return resource, nil
}

func (s *fakePubSubResourceStore) DeleteSubscriptionGroup(_ context.Context, id string) error {
	delete(fakeSubscriptionGroups[s], id)
	return nil
}

func (s *fakePubSubResourceStore) PutPublisherGroup(_ context.Context, resource pubsubmodel.PublisherGroup) error {
	if fakePublisherGroups[s] == nil {
		fakePublisherGroups[s] = map[string]pubsubmodel.PublisherGroup{}
	}
	resource.ID = strings.TrimSpace(resource.ID)
	fakePublisherGroups[s][resource.ID] = resource
	return nil
}

func (s *fakePubSubResourceStore) GetPublisherGroup(_ context.Context, id string) (pubsubmodel.PublisherGroup, error) {
	resource, ok := fakePublisherGroups[s][strings.TrimSpace(id)]
	if !ok {
		return pubsubmodel.PublisherGroup{}, pubsubmodel.ErrNotFound
	}
	return resource, nil
}

func (s *fakePubSubResourceStore) DeletePublisherGroup(_ context.Context, id string) error {
	delete(fakePublisherGroups[s], id)
	return nil
}

func TestPubSubResourceAPIPublisherAndGroups(t *testing.T) {
	store := newFakePubSubResourceStore()
	router := resourceTestRouter(t, store)

	resp := requestResource(t, router, http.MethodPost, "/pubsub/publishers", `{"id":"stonks-live","channel":"market","type":"stonks","config":{"feed":"live"}}`)
	if resp.Code != http.StatusCreated || !strings.Contains(resp.Body.String(), `"id":"stonks-live"`) {
		t.Fatalf("POST Publisher status=%d body=%s", resp.Code, resp.Body.String())
	}

	resp = requestResource(t, router, http.MethodGet, "/pubsub/publishers", "")
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), "stonks-live") {
		t.Fatalf("list Publishers status=%d body=%s", resp.Code, resp.Body.String())
	}

	resp = requestResource(t, router, http.MethodPost, "/pubsub/subscription-groups", `{"id":"consumers","subscriberIDs":[]}`)
	if resp.Code != http.StatusCreated {
		t.Fatalf("POST SubscriptionGroup status=%d body=%s", resp.Code, resp.Body.String())
	}

	resp = requestResource(t, router, http.MethodPost, "/pubsub/publisher-groups", `{"id":"market-producers","publisherIDs":["stonks-live"]}`)
	if resp.Code != http.StatusCreated || !strings.Contains(resp.Body.String(), "stonks-live") {
		t.Fatalf("POST PublisherGroup status=%d body=%s", resp.Code, resp.Body.String())
	}

	resp = requestResource(t, router, http.MethodGet, "/pubsub/publisher-groups/market-producers", "")
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), "stonks-live") {
		t.Fatalf("GET PublisherGroup status=%d body=%s", resp.Code, resp.Body.String())
	}
}
