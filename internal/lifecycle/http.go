package lifecycle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
	"github.com/go-chi/chi/v5"
)

func Handler(service *Service) http.Handler {
	r := chi.NewRouter()
	r.Get("/policies", func(w http.ResponseWriter, _ *http.Request) {
		writePolicies(w)
	})
	r.Get("/registrations", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, service.List())
	})
	r.Post("/registrations", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req RegisterRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "Invalid lifecycle registration", http.StatusBadRequest)
			return
		}
		reg, err := service.Register(r.Context(), req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusCreated, reg)
	})
	r.Delete("/registrations/{deploymentID}", func(w http.ResponseWriter, r *http.Request) {
		if err := service.Delete(r.Context(), chi.URLParam(r, "deploymentID")); err != nil {
			http.Error(w, "Failed to cancel lifecycle registration", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return r
}

func writePolicies(w http.ResponseWriter) {
	names := []ratelimiter.LifecyclePolicyName{
		ratelimiter.LifecycleSmoke30S,
		ratelimiter.LifecycleSmoke1M,
		ratelimiter.LifecycleSandbox1D,
		ratelimiter.LifecycleSandbox3D,
		ratelimiter.LifecycleSandbox7D,
		ratelimiter.LifecycleSandbox14D,
		ratelimiter.LifecycleSandbox30D,
	}
	type policyResponse struct {
		Name            ratelimiter.LifecyclePolicyName `json:"name"`
		PolicyCode      string                          `json:"policy_code"`
		DurationSeconds int64                           `json:"duration_seconds"`
		Energy          uint16                          `json:"energy"`
	}
	out := make([]policyResponse, 0, len(names))
	for _, name := range names {
		policy, err := ratelimiter.NamedLifecyclePolicy(name)
		if err != nil {
			http.Error(w, "Failed to compile lifecycle policy", http.StatusInternalServerError)
			return
		}
		code, err := ratelimiter.EncodePolicy(policy)
		if err != nil {
			http.Error(w, "Failed to encode lifecycle policy", http.StatusInternalServerError)
			return
		}
		out = append(out, policyResponse{
			Name:            name,
			PolicyCode:      fmt.Sprintf("%d", uint64(code)),
			DurationSeconds: int64(policy.Duration.Duration() / time.Second),
			Energy:          policy.EnergyCost(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		fmt.Printf("Error encoding lifecycle response: %v\n", err)
	}
}
