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
 "github.com/xd-dash/logma/serverless/authhttp"
)

const maxPubSubResourceBodyBytes = 1 << 20

type pubSubResourceStore interface {
	PutChannel(context.Context, pubsubmodel.Channel) error
	GetChannel(context.Context, string) (pubsubmodel.Channel, error)
	DeleteChannel(context.Context, string) error
	ChannelIDs(context.Context) ([]string, error)

	PutCallback(context.Context, pubsubmodel.Callback) error
	GetCallback(context.Context, string) (pubsubmodel.Callback, error)
	DeleteCallback(context.Context, string) error
	CallbackIDs(context.Context) ([]string, error)

	PutSubscriber(context.Context, pubsubmodel.Subscriber) error
	GetSubscriber(context.Context, string) (pubsubmodel.Subscriber, error)
	DeleteSubscriber(context.Context, string) error
	SubscriberIDs(context.Context) ([]string, error)

	PutPublisher(context.Context, pubsubmodel.Publisher) error
	GetPublisher(context.Context, string) (pubsubmodel.Publisher, error)
	DeletePublisher(context.Context, string) error
	PublisherIDs(context.Context) ([]string, error)

	PutSubscriptionGroup(context.Context, pubsubmodel.SubscriptionGroup) error
	GetSubscriptionGroup(context.Context, string) (pubsubmodel.SubscriptionGroup, error)
	DeleteSubscriptionGroup(context.Context, string) error
	SubscriptionGroupIDs(context.Context) ([]string, error)

	PutPublisherGroup(context.Context, pubsubmodel.PublisherGroup) error
	GetPublisherGroup(context.Context, string) (pubsubmodel.PublisherGroup, error)
	DeletePublisherGroup(context.Context, string) error
	PublisherGroupIDs(context.Context) ([]string, error)
}

// PublisherReconciler is supplied by runtime composition. The resource router
// never manufactures transport authority from the graph-store credential.
type PublisherReconciler interface {
	Reconcile(context.Context, string) error
}

type pubSubResourceAPI struct {
	store      func() (pubSubResourceStore, error)
	reconciler PublisherReconciler
	projectResolver ProjectBindingResolver
	projectStore func(string) (pubSubResourceStore, error)
}

func newPubSubResourceAPI() *pubSubResourceAPI {
	store, err := newPubSubResourceRedisStore()
	return &pubSubResourceAPI{
		store: func() (pubSubResourceStore, error) {
			return store, err
		},
	}
}

func NewPubSubResourceRouter() http.Handler {
	return newPubSubResourceRouter(newPubSubResourceAPI())
}

func NewPubSubResourceRouterWithPublisherReconciler(reconciler PublisherReconciler) http.Handler {
	api := newPubSubResourceAPI()
	api.reconciler = reconciler
	return newPubSubResourceRouter(api)
}

// NewPubSubResourceRouterWithProjectResolver installs verified project binding resolution.
// The resolver owns authentication and verification; user-provided headers are never authority.
func NewPubSubResourceRouterWithProjectResolver(resolver ProjectBindingResolver) http.Handler {
 api := newPubSubResourceAPI()
 api.projectResolver = resolver
 api.projectStore = newPubSubResourceRedisStoreForProject
 return newPubSubResourceRouter(api)
}

// NewPubSubResourceRouterWithHTTPBinding composes shared bearer authentication
// with the existing resource-level Prajapati policy checks. The resolver is
// injected by the deployment and must verify actual credentials and revocation.
func NewPubSubResourceRouterWithHTTPBinding(audience string, resolver authhttp.Resolver) http.Handler {
 api := newPubSubResourceAPI()
 api.projectResolver = authhttp.ContextResolver{}
 api.projectStore = newPubSubResourceRedisStoreForProject
 return authhttp.Middleware(audience, resolver)(newPubSubResourceRouter(api))
}

func newPubSubResourceRouter(api *pubSubResourceAPI) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(authenticateAPIKey)
	if api.projectResolver != nil { r.Use(projectAuthorizationMiddleware(api.projectResolver)) }
	api.routes(r)
	return r
}

func (a *pubSubResourceAPI) authorizedRoute(action, kind, param string, handler http.HandlerFunc) http.HandlerFunc {
 return func(w http.ResponseWriter, r *http.Request) {
  if a.projectResolver != nil {
   id := param
   if param != "collection" { id = chi.URLParam(r, param) }
   if !authorizeProjectResource(w, r, action, kind, id) { return }
  }
  handler(w, r)
 }
}

func (a *pubSubResourceAPI) storeForRequest(r *http.Request) (pubSubResourceStore, error) {
 if a.projectResolver == nil { return a.store() }
 binding, ok := r.Context().Value(projectBindingContextKey{}).(VerifiedProjectBinding)
 if !ok || binding == nil || a.projectStore == nil { return nil, errors.New("project-scoped storage unavailable") }
 return a.projectStore(binding.ProjectID())
}

func (a *pubSubResourceAPI) routes(r chi.Router) {
	r.Route("/pubsub", func(r chi.Router) {
		r.Get("/channels", a.authorizedRoute("list", "channels", "collection", a.listChannels))
		r.Post("/channels", a.authorizedRoute("write", "channels", "collection", a.putChannel))
		r.Get("/channels/{name}", a.authorizedRoute("read", "channels", "name", a.getChannel))
		r.Delete("/channels/{name}", a.authorizedRoute("delete", "channels", "name", a.deleteChannel))

		r.Get("/callbacks", a.authorizedRoute("list", "callbacks", "collection", a.listCallbacks))
		r.Post("/callbacks", a.authorizedRoute("write", "callbacks", "collection", a.putCallback))
		r.Get("/callbacks/{id}", a.authorizedRoute("read", "callbacks", "id", a.getCallback))
		r.Delete("/callbacks/{id}", a.authorizedRoute("delete", "callbacks", "id", a.deleteCallback))

		r.Get("/subscribers", a.authorizedRoute("list", "subscribers", "collection", a.listSubscribers))
		r.Post("/subscribers", a.authorizedRoute("write", "subscribers", "collection", a.putSubscriber))
		r.Get("/subscribers/{id}", a.authorizedRoute("read", "subscribers", "id", a.getSubscriber))
		r.Delete("/subscribers/{id}", a.authorizedRoute("delete", "subscribers", "id", a.deleteSubscriber))

		r.Get("/publishers", a.authorizedRoute("list", "publishers", "collection", a.listPublishers))
		r.Post("/publishers", a.authorizedRoute("write", "publishers", "collection", a.putPublisher))
		r.Get("/publishers/{id}", a.authorizedRoute("read", "publishers", "id", a.getPublisher))
		r.Delete("/publishers/{id}", a.authorizedRoute("delete", "publishers", "id", a.deletePublisher))
		r.Post("/publishers/{id}/reconcile", a.authorizedRoute("reconcile", "publishers", "id", a.reconcilePublisher))

		r.Get("/subscription-groups", a.authorizedRoute("list", "subscription-groups", "collection", a.listSubscriptionGroups))
		r.Post("/subscription-groups", a.authorizedRoute("write", "subscription-groups", "collection", a.putSubscriptionGroup))
		r.Get("/subscription-groups/{id}", a.authorizedRoute("read", "subscription-groups", "id", a.getSubscriptionGroup))
		r.Delete("/subscription-groups/{id}", a.authorizedRoute("delete", "subscription-groups", "id", a.deleteSubscriptionGroup))

		r.Get("/publisher-groups", a.authorizedRoute("list", "publisher-groups", "collection", a.listPublisherGroups))
		r.Post("/publisher-groups", a.authorizedRoute("write", "publisher-groups", "collection", a.putPublisherGroup))
		r.Get("/publisher-groups/{id}", a.authorizedRoute("read", "publisher-groups", "id", a.getPublisherGroup))
		r.Delete("/publisher-groups/{id}", a.authorizedRoute("delete", "publisher-groups", "id", a.deletePublisherGroup))
	})
}

func (a *pubSubResourceAPI) putChannel(w http.ResponseWriter, r *http.Request) {
	var resource pubsubmodel.Channel
	a.put(w, r, &resource,
		func(store pubSubResourceStore) error { return store.PutChannel(r.Context(), resource) },
		func(store pubSubResourceStore) (any, error) { return store.GetChannel(r.Context(), resource.Name) },
	)
}

func (a *pubSubResourceAPI) getChannel(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w, r)
	if !ok {
		return
	}
	resource, err := store.GetChannel(r.Context(), chi.URLParam(r, "name"))
	writeGetResult(w, resource, err)
}

func (a *pubSubResourceAPI) deleteChannel(w http.ResponseWriter, r *http.Request) {
	a.delete(w, r, func(store pubSubResourceStore) error {
		return store.DeleteChannel(r.Context(), chi.URLParam(r, "name"))
	}, "failed to delete Channel")
}

func (a *pubSubResourceAPI) listChannels(w http.ResponseWriter, r *http.Request) {
	a.list(w, r, func(store pubSubResourceStore) ([]string, error) { return store.ChannelIDs(r.Context()) })
}

func (a *pubSubResourceAPI) putCallback(w http.ResponseWriter, r *http.Request) {
	var resource pubsubmodel.Callback
	a.put(w, r, &resource,
		func(store pubSubResourceStore) error { return store.PutCallback(r.Context(), resource) },
		func(store pubSubResourceStore) (any, error) { return store.GetCallback(r.Context(), resource.ID) },
	)
}

func (a *pubSubResourceAPI) getCallback(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w, r)
	if !ok {
		return
	}
	resource, err := store.GetCallback(r.Context(), chi.URLParam(r, "id"))
	writeGetResult(w, resource, err)
}

func (a *pubSubResourceAPI) deleteCallback(w http.ResponseWriter, r *http.Request) {
	a.delete(w, r, func(store pubSubResourceStore) error {
		return store.DeleteCallback(r.Context(), chi.URLParam(r, "id"))
	}, "failed to delete Callback")
}

func (a *pubSubResourceAPI) listCallbacks(w http.ResponseWriter, r *http.Request) {
	a.list(w, r, func(store pubSubResourceStore) ([]string, error) { return store.CallbackIDs(r.Context()) })
}

func (a *pubSubResourceAPI) putSubscriber(w http.ResponseWriter, r *http.Request) {
	var resource pubsubmodel.Subscriber
	a.put(w, r, &resource,
		func(store pubSubResourceStore) error { return store.PutSubscriber(r.Context(), resource) },
		func(store pubSubResourceStore) (any, error) { return store.GetSubscriber(r.Context(), resource.ID) },
	)
}

func (a *pubSubResourceAPI) getSubscriber(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w, r)
	if !ok {
		return
	}
	resource, err := store.GetSubscriber(r.Context(), chi.URLParam(r, "id"))
	writeGetResult(w, resource, err)
}

func (a *pubSubResourceAPI) deleteSubscriber(w http.ResponseWriter, r *http.Request) {
	a.delete(w, r, func(store pubSubResourceStore) error {
		return store.DeleteSubscriber(r.Context(), chi.URLParam(r, "id"))
	}, "failed to delete Subscriber")
}

func (a *pubSubResourceAPI) listSubscribers(w http.ResponseWriter, r *http.Request) {
	a.list(w, r, func(store pubSubResourceStore) ([]string, error) { return store.SubscriberIDs(r.Context()) })
}

func (a *pubSubResourceAPI) putPublisher(w http.ResponseWriter, r *http.Request) {
	var resource pubsubmodel.Publisher
	a.put(w, r, &resource,
		func(store pubSubResourceStore) error { return store.PutPublisher(r.Context(), resource) },
		func(store pubSubResourceStore) (any, error) { return store.GetPublisher(r.Context(), resource.ID) },
	)
}

func (a *pubSubResourceAPI) getPublisher(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w, r)
	if !ok {
		return
	}
	resource, err := store.GetPublisher(r.Context(), chi.URLParam(r, "id"))
	writeGetResult(w, resource, err)
}

func (a *pubSubResourceAPI) deletePublisher(w http.ResponseWriter, r *http.Request) {
	a.delete(w, r, func(store pubSubResourceStore) error {
		return store.DeletePublisher(r.Context(), chi.URLParam(r, "id"))
	}, "failed to delete Publisher")
}

func (a *pubSubResourceAPI) listPublishers(w http.ResponseWriter, r *http.Request) {
	a.list(w, r, func(store pubSubResourceStore) ([]string, error) { return store.PublisherIDs(r.Context()) })
}

func (a *pubSubResourceAPI) reconcilePublisher(w http.ResponseWriter, r *http.Request) {
	if a.reconciler == nil {
		http.Error(w, "publisher reconciliation is not configured", http.StatusServiceUnavailable)
		return
	}
	if err := a.reconciler.Reconcile(r.Context(), chi.URLParam(r, "id")); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *pubSubResourceAPI) putSubscriptionGroup(w http.ResponseWriter, r *http.Request) {
	var resource pubsubmodel.SubscriptionGroup
	a.put(w, r, &resource,
		func(store pubSubResourceStore) error { return store.PutSubscriptionGroup(r.Context(), resource) },
		func(store pubSubResourceStore) (any, error) {
			return store.GetSubscriptionGroup(r.Context(), resource.ID)
		},
	)
}

func (a *pubSubResourceAPI) getSubscriptionGroup(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w, r)
	if !ok {
		return
	}
	resource, err := store.GetSubscriptionGroup(r.Context(), chi.URLParam(r, "id"))
	writeGetResult(w, resource, err)
}

func (a *pubSubResourceAPI) deleteSubscriptionGroup(w http.ResponseWriter, r *http.Request) {
	a.delete(w, r, func(store pubSubResourceStore) error {
		return store.DeleteSubscriptionGroup(r.Context(), chi.URLParam(r, "id"))
	}, "failed to delete SubscriptionGroup")
}

func (a *pubSubResourceAPI) listSubscriptionGroups(w http.ResponseWriter, r *http.Request) {
	a.list(w, r, func(store pubSubResourceStore) ([]string, error) { return store.SubscriptionGroupIDs(r.Context()) })
}

func (a *pubSubResourceAPI) putPublisherGroup(w http.ResponseWriter, r *http.Request) {
	var resource pubsubmodel.PublisherGroup
	a.put(w, r, &resource,
		func(store pubSubResourceStore) error { return store.PutPublisherGroup(r.Context(), resource) },
		func(store pubSubResourceStore) (any, error) { return store.GetPublisherGroup(r.Context(), resource.ID) },
	)
}

func (a *pubSubResourceAPI) getPublisherGroup(w http.ResponseWriter, r *http.Request) {
	store, ok := a.resourceStore(w, r)
	if !ok {
		return
	}
	resource, err := store.GetPublisherGroup(r.Context(), chi.URLParam(r, "id"))
	writeGetResult(w, resource, err)
}

func (a *pubSubResourceAPI) deletePublisherGroup(w http.ResponseWriter, r *http.Request) {
	a.delete(w, r, func(store pubSubResourceStore) error {
		return store.DeletePublisherGroup(r.Context(), chi.URLParam(r, "id"))
	}, "failed to delete PublisherGroup")
}

func (a *pubSubResourceAPI) listPublisherGroups(w http.ResponseWriter, r *http.Request) {
	a.list(w, r, func(store pubSubResourceStore) ([]string, error) { return store.PublisherGroupIDs(r.Context()) })
}

func (a *pubSubResourceAPI) put(
	w http.ResponseWriter,
	r *http.Request,
	target interface{ Validate() error },
	persist func(pubSubResourceStore) error,
	read func(pubSubResourceStore) (any, error),
) {
	if !decodeResource(w, r, target) {
		return
	}
	if err := target.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	store, ok := a.resourceStore(w, r)
	if !ok {
		return
	}
	if err := persist(store); err != nil {
		writeGraphMutationError(w, err, "failed to persist resource")
		return
	}
	stored, err := read(store)
	if err != nil {
		http.Error(w, "failed to read persisted resource", http.StatusInternalServerError)
		return
	}
	writeResource(w, http.StatusCreated, stored)
}

func (a *pubSubResourceAPI) delete(w http.ResponseWriter, r *http.Request, fn func(pubSubResourceStore) error, message string) {
	store, ok := a.resourceStore(w, r)
	if !ok {
		return
	}
	if err := fn(store); err != nil {
		writeGraphMutationError(w, err, message)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *pubSubResourceAPI) list(w http.ResponseWriter, r *http.Request, fn func(pubSubResourceStore) ([]string, error)) {
	store, ok := a.resourceStore(w, r)
	if !ok {
		return
	}
	ids, err := fn(store)
	if err != nil {
		http.Error(w, "failed to list resources", http.StatusInternalServerError)
		return
	}
	writeResource(w, http.StatusOK, ids)
}

func (a *pubSubResourceAPI) resourceStore(w http.ResponseWriter, r *http.Request) (pubSubResourceStore, bool) {
	store, err := a.storeForRequest(r)
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
	payload, err := json.Marshal(resource)
	if err != nil {
		http.Error(w, "failed to encode resource", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(payload, '\n'))
}
