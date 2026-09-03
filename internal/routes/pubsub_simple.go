package routes

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

// simplePubSubAPI is the operator-facing facade over the normalized Pub/Sub
// resource graph. The graph remains available through /pubsub/* for advanced
// control-plane use; ordinary callers should not have to manually create a
// Channel, Callback, and Subscriber for the common webhook-subscription case.
type simplePubSubAPI struct {
	store func() (pubSubResourceStore, error)
	newID func() (string, error)
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

// NewSimplePubSubRouter exposes the small, task-oriented API. It deliberately
// composes the existing resource store instead of introducing a second storage
// model or weakening the v2 ACL/keyspace boundary.
func NewSimplePubSubRouter() http.Handler {
	return newSimplePubSubRouter(newSimplePubSubAPI())
}

func newSimplePubSubRouter(api *simplePubSubAPI) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(authenticateAPIKey)
	r.Post("/subscribe", api.subscribe)
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
	// These declarations are intentionally idempotent building blocks rather
	// than a second persistence transaction. RedisStore owns each resource's
	// graph integrity; a later reconciliation layer owns runtime activation.
	if err := store.PutChannel(r.Context(), channel); err != nil {
		writeGraphMutationError(w, err, "failed to ensure Channel")
		return
	}
	if err := store.PutCallback(r.Context(), callback); err != nil {
		writeGraphMutationError(w, err, "failed to ensure Callback")
		return
	}
	if err := store.PutSubscriber(r.Context(), subscriber); err != nil {
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

func (a *simplePubSubAPI) state(w http.ResponseWriter, r *http.Request) {
	store, err := a.store()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
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
