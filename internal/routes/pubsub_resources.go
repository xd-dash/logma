package routes

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

const maxPubSubResourceBodyBytes = 1 << 20

type pubSubResourceStore interface {
	PutChannel(context.Context, pubsubmodel.Channel) error
	GetChannel(context.Context, string) (pubsubmodel.Channel, error)
	DeleteChannel(context.Context, string) error
	PutCallback(context.Context, pubsubmodel.Callback) error
	GetCallback(context.Context, string) (pubsubmodel.Callback, error)
	DeleteCallback(context.Context, string) error
	PutSubscriber(context.Context, pubsubmodel.Subscriber) error
	GetSubscriber(context.Context, string) (pubsubmodel.Subscriber, error)
	DeleteSubscriber(context.Context, string) error
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
		r.Delete("/channels/{name}", a.deleteChannel)
		r.Post("/callbacks", a.putCallback)
		r.Get("/callbacks/{id}", a.getCallback)
		r.Delete("/callbacks/{id}", a.deleteCallback)
		r.Post("/subscribers", a.putSubscriber)
		r.Get("/subscribers/{id}", a.getSubscriber)
		r.Delete("/subscribers/{id}", a.deleteSubscriber)
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
	stored, err := store.GetChannel(r.Context(), resource.Name)
	if err != nil {
		http.Error(w, "failed to read persisted Channel", http.StatusInternalServerError)
		return
	}
	writeResource(w, http.StatusCreated, stored)
}

func (a *pubSubResourceAPI) getChannel(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	resource, err := store.GetChannel(r.Context(), chi.URLParam(r, "name"))
	writeGetResult(w, resource, err)
}

func (a *pubSubResourceAPI) deleteChannel(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	if err := store.DeleteChannel(r.Context(), chi.URLParam(r, "name")); err != nil {
		writeGraphMutationError(w, err, "failed to delete Channel")
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
	stored, err := store.GetCallback(r.Context(), resource.ID)
	if err != nil {
		http.Error(w, "failed to read persisted Callback", http.StatusInternalServerError)
		return
	}
	writeResource(w, http.StatusCreated, stored)
}

func (a *pubSubResourceAPI) getCallback(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	resource, err := store.GetCallback(r.Context(), chi.URLParam(r, "id"))
	writeGetResult(w, resource, err)
}

func (a *pubSubResourceAPI) deleteCallback(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	if err := store.DeleteCallback(r.Context(), chi.URLParam(r, "id")); err != nil {
		writeGraphMutationError(w, err, "failed to delete Callback")
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
		writeGraphMutationError(w, err, "failed to persist Subscriber")
		return
	}
	stored, err := store.GetSubscriber(r.Context(), resource.ID)
	if err != nil {
		http.Error(w, "failed to read persisted Subscriber", http.StatusInternalServerError)
		return
	}
	writeResource(w, http.StatusCreated, stored)
}

func (a *pubSubResourceAPI) getSubscriber(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	resource, err := store.GetSubscriber(r.Context(), chi.URLParam(r, "id"))
	writeGetResult(w, resource, err)
}

func (a *pubSubResourceAPI) deleteSubscriber(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w)
	if !ok {
		return
	}
	if err := store.DeleteSubscriber(r.Context(), chi.URLParam(r, "id")); err != nil {
		http.Error(w, "failed to delete Subscriber", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
	r.Body = http.MaxBytesReader(w, r.Body, maxPubSubResourceBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		http.Error(w, "invalid JSON resource", http.StatusBadRequest)
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		http.Error(w, "invalid JSON resource", http.StatusBadRequest)
		return false
	}
	return true
}

func writeGraphMutationError(w http.ResponseWriter, err error, internalMessage string) {
	if errors.Is(err, pubsubmodel.ErrMissingReference) || errors.Is(err, pubsubmodel.ErrReferenced) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	http.Error(w, internalMessage, http.StatusInternalServerError)
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
