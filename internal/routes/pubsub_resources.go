package routes

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

type pubSubResourceStore interface {
	PutChannel(context.Context, pubsubmodel.Channel) error
	GetChannel(context.Context, string) (pubsubmodel.Channel, error)
	PutCallback(context.Context, pubsubmodel.Callback) error
	GetCallback(context.Context, string) (pubsubmodel.Callback, error)
	PutSubscriber(context.Context, pubsubmodel.Subscriber) error
	GetSubscriber(context.Context, string) (pubsubmodel.Subscriber, error)
}

type pubSubResourceAPI struct {
	store func() (pubSubResourceStore, error)
}

func newPubSubResourceAPI() *pubSubResourceAPI {
	store, err := newPubSubResourceRedisStore()
	return &pubSubResourceAPI{store: func() (pubSubResourceStore, error) {
		return store, err
	}}
}

// NewPubSubResourceRouter exposes the additive resource API independently of
// the legacy /channels compatibility surface. The canonical router can mount
// this after the resource contract has been qualified without changing legacy
// active_subscriptions behavior in the same step.
func NewPubSubResourceRouter() http.Handler {
	return newPubSubResourceRouter(newPubSubResourceAPI())
}

func newPubSubResourceRouter(api *pubSubResourceAPI) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(authenticateAPIKey)
	api.routes(r)
	return r
}

func (a *pubSubResourceAPI) routes(r chi.Router) {
	r.Route("/pubsub", func(r chi.Router) {
		r.Post("/channels", a.putChannel)
		r.Get("/channels/{name}", a.getChannel)
		r.Post("/callbacks", a.putCallback)
		r.Get("/callbacks/{id}", a.getCallback)
		r.Post("/subscribers", a.putSubscriber)
		r.Get("/subscribers/{id}", a.getSubscriber)
	})
}

func (a *pubSubResourceAPI) putChannel(w http.ResponseWriter, r *http.Request) {
	var resource pubsubmodel.Channel
	if !decodeResource(w, r, &resource) {
		return
	}
	if err := resource.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	if err := store.PutChannel(r.Context(), resource); err != nil {
		http.Error(w, "failed to persist Channel", http.StatusInternalServerError)
		return
	}
	writeResource(w, http.StatusCreated, resource)
}

func (a *pubSubResourceAPI) getChannel(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	resource, err := store.GetChannel(r.Context(), chi.URLParam(r, "name"))
	writeGetResult(w, resource, err)
}

func (a *pubSubResourceAPI) putCallback(w http.ResponseWriter, r *http.Request) {
	var resource pubsubmodel.Callback
	if !decodeResource(w, r, &resource) {
		return
	}
	if err := resource.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	if err := store.PutCallback(r.Context(), resource); err != nil {
		http.Error(w, "failed to persist Callback", http.StatusInternalServerError)
		return
	}
	writeResource(w, http.StatusCreated, resource)
}

func (a *pubSubResourceAPI) getCallback(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	resource, err := store.GetCallback(r.Context(), chi.URLParam(r, "id"))
	writeGetResult(w, resource, err)
}

func (a *pubSubResourceAPI) putSubscriber(w http.ResponseWriter, r *http.Request) {
	var resource pubsubmodel.Subscriber
	if !decodeResource(w, r, &resource) {
		return
	}
	if err := resource.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	if err := store.PutSubscriber(r.Context(), resource); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeResource(w, http.StatusCreated, resource)
}

func (a *pubSubResourceAPI) getSubscriber(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	resource, err := store.GetSubscriber(r.Context(), chi.URLParam(r, "id"))
	writeGetResult(w, resource, err)
}

func (a *pubSubResourceAPI) resourceStore(w http.ResponseWriter) (pubSubResourceStore, bool) {
	store, err := a.store()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return nil, false
	}
	return store, true
}

func decodeResource(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		http.Error(w, "invalid JSON resource", http.StatusBadRequest)
		return false
	}
	return true
}

func writeGetResult[T any](w http.ResponseWriter, resource T, err error) {
	if err != nil {
		if errors.Is(err, pubsubmodel.ErrNotFound) {
			http.Error(w, "resource not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to read resource", http.StatusInternalServerError)
		return
	}
	writeResource(w, http.StatusOK, resource)
}

func writeResource(w http.ResponseWriter, status int, resource any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resource)
}
