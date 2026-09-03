package routes

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

// SubscriptionController is supplied only by a runtime composition that
// actually owns subscription transport authority. The simple graph facade does
// not manufacture SUBSCRIBE authority from its resource-store credential.
type SubscriptionController interface {
	ActivateSubscription(context.Context, string) error
	ShutdownSubscription(context.Context, string) error
}

// simpleSubscriptionComposer is a provider-owned atomic primitive for the
// common Channel + webhook Callback + Subscriber operation. Keeping atomicity
// below the facade prevents a late conflict from leaving partial declarations
// or overwriting an independently managed Callback.
type simpleSubscriptionComposer interface {
	CreateWebhookSubscription(context.Context, pubsubmodel.Channel, pubsubmodel.Callback, pubsubmodel.Subscriber) error
}

// simplePubSubAPI is the operator-facing facade over the normalized Pub/Sub
// resource graph. The graph remains available through /pubsub/* for advanced
// control-plane use; ordinary callers should not have to manually create a
// Channel, Callback, and Subscriber for the common webhook-subscription case.
type simplePubSubAPI struct {
	store      func() (pubSubResourceStore, error)
	newID      func() (string, error)
	controller SubscriptionController
}

type simpleSubscribeRequest struct {
	ID          string `json:"id,omitempty"`
	CallbackID  string `json:"callbackID,omitempty"`
	Channel     string `json:"channel"`
	CallbackURL string `json:"callbackURL"`
}

type simpleSubscribeResponse struct {
	SubscriptionID string `json:"subscriptionID"`
	Channel        string `json:"channel"`
	CallbackID     string `json:"callbackID"`
	CallbackURL    string `json:"callbackURL"`
}

type simpleGroupRequest struct {
	ID            string   `json:"id"`
	Subscriptions []string `json:"subscriptions"`
}

type simpleGroupResponse struct {
	ID            string   `json:"id"`
	Subscriptions []string `json:"subscriptions"`
}

type simpleGroupOperationResponse struct {
	Group     string   `json:"group"`
	Completed []string `json:"completed"`
	Missing   []string `json:"missing,omitempty"`
	Failed    []string `json:"failed,omitempty"`
}

type simpleState struct {
	Channels        []string `json:"channels"`
	Subscriptions   []string `json:"subscriptions"`
	Publishers      []string `json:"publishers"`
	Groups          []string `json:"groups"`
	PublisherGroups []string `json:"publisherGroups"`
}

func newSimplePubSubAPI() *simplePubSubAPI {
	store, err := newPubSubResourceRedisStore()
	return &simplePubSubAPI{
		store: func() (pubSubResourceStore, error) { return store, err },
		newID: newSimpleResourceID,
	}
}

// NewSimplePubSubRouter exposes the small, task-oriented graph API. Runtime
// operations return unavailable until an explicitly authorized controller is
// composed.
func NewSimplePubSubRouter() http.Handler {
	return newSimplePubSubRouter(newSimplePubSubAPI())
}

// NewSimplePubSubRouterWithSubscriptionController composes the same small API
// with explicitly supplied runtime authority.
func NewSimplePubSubRouterWithSubscriptionController(controller SubscriptionController) http.Handler {
	api := newSimplePubSubAPI()
	api.controller = controller
	return newSimplePubSubRouter(api)
}

func newSimplePubSubRouter(api *simplePubSubAPI) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(authenticateAPIKey)
	r.Post("/subscribe", api.subscribe)
	r.Post("/groups", api.putGroup)
	r.Get("/groups/{id}", api.getGroup)
	r.Delete("/groups/{id}", api.deleteGroup)
	r.Post("/groups/{id}/activate", api.activateGroup)
	r.Post("/groups/{id}/shutdown", api.shutdownGroup)
	r.Get("/state", api.state)
	return r
}

func (a *simplePubSubAPI) subscribe(w http.ResponseWriter, r *http.Request) {
	var request simpleSubscribeRequest
	if !decodeResource(w, r, &request) {
		return
	}
	request.Channel = strings.TrimSpace(request.Channel)
	request.CallbackURL = strings.TrimSpace(request.CallbackURL)
	request.ID = strings.TrimSpace(request.ID)
	request.CallbackID = strings.TrimSpace(request.CallbackID)
	if request.Channel == "" {
		http.Error(w, "channel is required", http.StatusBadRequest)
		return
	}
	if request.CallbackURL == "" {
		http.Error(w, "callbackURL is required", http.StatusBadRequest)
		return
	}
	if request.ID == "" {
		id, err := a.newID()
		if err != nil {
			http.Error(w, "failed to allocate subscription id", http.StatusInternalServerError)
			return
		}
		request.ID = id
	}
	if request.CallbackID == "" {
		request.CallbackID = request.ID + "-callback"
	}

	channel := pubsubmodel.Channel{Name: request.Channel}
	callback := pubsubmodel.Callback{
		ID:   request.CallbackID,
		Type: pubsubmodel.CallbackWebhook,
		Webhook: &pubsubmodel.WebhookCallback{
			CallbackURL: request.CallbackURL,
		},
	}
	subscriber := pubsubmodel.Subscriber{
		ID:          request.ID,
		Channel:     request.Channel,
		CallbackIDs: []string{request.CallbackID},
	}
	if err := channel.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := callback.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := subscriber.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	store, err := a.store()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	composer, ok := store.(simpleSubscriptionComposer)
	if !ok {
		http.Error(w, "atomic subscription composition is not configured", http.StatusServiceUnavailable)
		return
	}
	if err := composer.CreateWebhookSubscription(r.Context(), channel, callback, subscriber); err != nil {
		if errors.Is(err, pubsubmodel.ErrAlreadyExists) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeGraphMutationError(w, err, "failed to create Subscription")
		return
	}

	writeResource(w, http.StatusCreated, simpleSubscribeResponse{
		SubscriptionID: request.ID,
		Channel:        request.Channel,
		CallbackID:     request.CallbackID,
		CallbackURL:    request.CallbackURL,
	})
}

func (a *simplePubSubAPI) putGroup(w http.ResponseWriter, r *http.Request) {
	var request simpleGroupRequest
	if !decodeResource(w, r, &request) {
		return
	}
	group := pubsubmodel.SubscriptionGroup{ID: request.ID, SubscriberIDs: request.Subscriptions}
	if err := group.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	if err := store.PutSubscriptionGroup(r.Context(), group); err != nil {
		writeGraphMutationError(w, err, "failed to persist Group")
		return
	}
	stored, err := store.GetSubscriptionGroup(r.Context(), group.ID)
	if err != nil {
		http.Error(w, "failed to read persisted Group", http.StatusInternalServerError)
		return
	}
	writeResource(w, http.StatusCreated, simpleGroupResponse{ID: stored.ID, Subscriptions: stored.SubscriberIDs})
}

func (a *simplePubSubAPI) getGroup(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	group, err := store.GetSubscriptionGroup(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeGetResult(w, group, err)
		return
	}
	writeResource(w, http.StatusOK, simpleGroupResponse{ID: group.ID, Subscriptions: group.SubscriberIDs})
}

func (a *simplePubSubAPI) deleteGroup(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	if err := store.DeleteSubscriptionGroup(r.Context(), chi.URLParam(r, "id")); err != nil {
		writeGraphMutationError(w, err, "failed to delete Group")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *simplePubSubAPI) activateGroup(w http.ResponseWriter, r *http.Request) {
	a.runGroupOperation(w, r, func(ctx context.Context, id string) error {
		return a.controller.ActivateSubscription(ctx, id)
	})
}

func (a *simplePubSubAPI) shutdownGroup(w http.ResponseWriter, r *http.Request) {
	a.runGroupOperation(w, r, func(ctx context.Context, id string) error {
		return a.controller.ShutdownSubscription(ctx, id)
	})
}

func (a *simplePubSubAPI) runGroupOperation(w http.ResponseWriter, r *http.Request, operation func(context.Context, string) error) {
	if a.controller == nil {
		http.Error(w, "subscription runtime control is not configured", http.StatusServiceUnavailable)
		return
	}
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	group, err := store.GetSubscriptionGroup(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeGetResult(w, group, err)
		return
	}
	response := simpleGroupOperationResponse{Group: group.ID}
	for _, id := range group.SubscriberIDs {
		if err := operation(r.Context(), id); err != nil {
			if errors.Is(err, pubsubmodel.ErrNotFound) {
				response.Missing = append(response.Missing, id)
				continue
			}
			response.Failed = append(response.Failed, id)
			continue
		}
		response.Completed = append(response.Completed, id)
	}
	writeResource(w, http.StatusOK, response)
}

func (a *simplePubSubAPI) state(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	channels, err := store.ChannelIDs(r.Context())
	if err != nil {
		http.Error(w, "failed to list channels", http.StatusInternalServerError)
		return
	}
	subscriptions, err := store.SubscriberIDs(r.Context())
	if err != nil {
		http.Error(w, "failed to list subscriptions", http.StatusInternalServerError)
		return
	}
	publishers, err := store.PublisherIDs(r.Context())
	if err != nil {
		http.Error(w, "failed to list publishers", http.StatusInternalServerError)
		return
	}
	groups, err := store.SubscriptionGroupIDs(r.Context())
	if err != nil {
		http.Error(w, "failed to list groups", http.StatusInternalServerError)
		return
	}
	publisherGroups, err := store.PublisherGroupIDs(r.Context())
	if err != nil {
		http.Error(w, "failed to list publisher groups", http.StatusInternalServerError)
		return
	}
	writeResource(w, http.StatusOK, simpleState{
		Channels:        channels,
		Subscriptions:   subscriptions,
		Publishers:      publishers,
		Groups:          groups,
		PublisherGroups: publisherGroups,
	})
}

func (a *simplePubSubAPI) resourceStore(w http.ResponseWriter) (pubSubResourceStore, bool) {
	store, err := a.store()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return nil, false
	}
	return store, true
}

func newSimpleResourceID() (string, error) {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(raw[:])
	if id == "" {
		return "", errors.New("empty generated id")
	}
	return id, nil
}
