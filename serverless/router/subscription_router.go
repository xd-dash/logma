package router

import (
	"crypto/subtle"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/serverless/pubsub"
)

// NewSubscriptionRouter keeps HTTP authority separate from the owned Redis client.
func NewSubscriptionRouter(client *redis.Client, channels []string, token string) (http.Handler, error) {
	if client == nil || token == "" || len(channels) == 0 {
		return nil, errors.New("subscription router requires client, channels and HTTP token")
	}
	allowed := make(map[string]bool, len(channels))
	for _, channel := range channels {
		if channel == "" {
			return nil, errors.New("subscription channel is empty")
		}
		allowed[channel] = true
	}
	holder := pubsub.NewHolder(func() *Runtime { return NewSubscriptionRuntime(client, channels) })
	return Build(func(r chi.Router) {
		r.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if subtle.ConstantTimeCompare([]byte(req.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				for _, channel := range requestedChannels(req) {
					if !allowed[channel] {
						http.Error(w, "channel denied", http.StatusForbidden)
						return
					}
				}
				next.ServeHTTP(w, req)
			})
		})
		r.Post("/run", runHandler(holder))
		r.Get("/events", eventsHandler(holder))
	}), nil
}
