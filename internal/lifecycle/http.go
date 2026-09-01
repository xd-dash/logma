package lifecycle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
)

func (r *Runtime) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", r.handleList)
	mux.HandleFunc("POST /", r.handleRegister)
	mux.HandleFunc("GET /policies", r.handlePolicies)
	mux.HandleFunc("DELETE /{deploymentID}", r.handleUnregister)
	return mux
}

func (r *Runtime) handleList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, r.List())
}

func (r *Runtime) handleRegister(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	var registration Registration
	if err := json.NewDecoder(req.Body).Decode(&registration); err != nil {
		http.Error(w, "Invalid lifecycle registration", http.StatusBadRequest)
		return
	}
	if err := r.Register(req.Context(), registration); err != nil {
		if strings.Contains(err.Error(), "conflicting lifecycle registration") {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stored, ok := r.Get(registration.DeploymentID)
	if !ok {
		http.Error(w, "Lifecycle registration disappeared", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, stored)
}

func (r *Runtime) handleUnregister(w http.ResponseWriter, req *http.Request) {
	deploymentID := req.PathValue("deploymentID")
	if deploymentID == "" {
		http.Error(w, "Missing deployment ID", http.StatusBadRequest)
		return
	}
	if err := r.Unregister(req.Context(), deploymentID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Runtime) handlePolicies(w http.ResponseWriter, _ *http.Request) {
	names := []ratelimiter.LifecyclePolicyName{
		ratelimiter.LifecycleSmoke30S,
		ratelimiter.LifecycleSmoke1M,
		ratelimiter.LifecycleSmoke10M,
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
