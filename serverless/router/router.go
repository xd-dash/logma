package router

import (
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/xd-dash/logma/serverless/pubsub"
)

// Build constructs the shared serverless HTTP shell and lets applications
// register their own routes without inheriting the standalone service's auth.
func Build(register func(r chi.Router)) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	register(r)
	return r
}

// NewRouter returns the independently deployable Logma serverless SSE service.
func NewRouter() http.Handler {
	holder := pubsub.NewHolder(NewRuntime)
	return Build(func(r chi.Router) {
		r.Use(requireRedisAuth)
		r.Post("/run", runHandler(holder))
		r.Get("/events", eventsHandler(holder))
	})
}

func requireRedisAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := os.Getenv("REDISCLI_AUTH")
		if auth != "" && r.Header.Get(pubsub.HeaderRedisAuth) != auth {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
