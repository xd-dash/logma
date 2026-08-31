package routes

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
)

func publishChannelHandler(w http.ResponseWriter, r *http.Request) {
	channel := strings.TrimSpace(chi.URLParam(r, "channelName"))
	if channel == "" {
		http.Error(w, "Channel name is required", http.StatusBadRequest)
		return
	}

	principal, _ := principalFromRequest(r)
	scoped, err := scopeChannelForPrincipal(principal, channel)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	var payload any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "Failed to encode publish body", http.StatusBadRequest)
		return
	}

	redisClient := client
	var owned *redis.Client
	if principal.Tenant != "" {
		owned = redis.NewClient(redisOptionsForCredentials(principal.Username, principal.Password))
		redisClient = owned
		defer owned.Close()
	}

	if err := redisClient.Publish(r.Context(), scoped, raw).Err(); err != nil {
		http.Error(w, "Redis publish failed: "+err.Error(), http.StatusForbidden)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"channel":   scoped,
		"published": true,
	})
}
