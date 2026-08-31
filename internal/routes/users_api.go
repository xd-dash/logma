package routes

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/dash-xd/ratelimiter/auth"
	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
)

const aclUserMetadataPrefix = "logma:acl:user:"

type aclUserRecord struct {
	Username string `json:"username"`
	Tenant   string `json:"tenant"`
	Profile  string `json:"profile"`
}

type aclUserRequest struct {
	Username string `json:"username,omitempty"`
	Tenant   string `json:"tenant"`
	Profile  string `json:"profile,omitempty"`
	Password string `json:"password,omitempty"`
}

type aclUserResponse struct {
	Username string `json:"username"`
	Tenant   string `json:"tenant"`
	Profile  string `json:"profile"`
	Password string `json:"password,omitempty"`
}

func aclUserMetadataKey(username string) string {
	return aclUserMetadataPrefix + username
}

func readACLUserRecord(ctx context.Context, username string) (aclUserRecord, error) {
	raw, err := client.Get(ctx, aclUserMetadataKey(username)).Result()
	if err != nil {
		return aclUserRecord{}, err
	}
	var record aclUserRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return aclUserRecord{}, err
	}
	return record, nil
}

func writeACLUserRecord(ctx context.Context, record aclUserRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return client.Set(ctx, aclUserMetadataKey(record.Username), raw, 0).Err()
}

func generateACLPassword() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func createACLUserHandler(w http.ResponseWriter, r *http.Request) {
	var request aclUserRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	request.Tenant = strings.TrimSpace(request.Tenant)
	if err := auth.ValidateIdentifier(request.Tenant); err != nil {
		http.Error(w, "Invalid tenant: "+err.Error(), http.StatusBadRequest)
		return
	}

	provider, providerErr := authProviderFromEnv()
	if providerErr != nil || provider == nil {
		http.Error(w, "Auth provider unavailable", http.StatusInternalServerError)
		return
	}
	policy, err := provider.Policy(request.Profile)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	username := strings.TrimSpace(request.Username)
	if username == "" {
		scope, scopeErr := provider.Scope(request.Tenant, "")
		if scopeErr != nil {
			http.Error(w, scopeErr.Error(), http.StatusBadRequest)
			return
		}
		username = scope.Username
	}
	if err := auth.ValidateIdentifier(username); err != nil {
		http.Error(w, "Invalid username: "+err.Error(), http.StatusBadRequest)
		return
	}
	if username == provider.AdminUser() {
		http.Error(w, "Cannot replace the application admin", http.StatusConflict)
		return
	}

	password := request.Password
	if password == "" {
		password, err = generateACLPassword()
		if err != nil {
			http.Error(w, "Failed to generate password", http.StatusInternalServerError)
			return
		}
	}

	if _, err := readACLUserRecord(r.Context(), username); err == nil {
		http.Error(w, "User already exists", http.StatusConflict)
		return
	} else if !errors.Is(err, redis.Nil) {
		http.Error(w, "Failed to inspect user metadata", http.StatusInternalServerError)
		return
	}

	rules, err := provider.Rules(auth.UserSpec{
		Tenant: request.Tenant, Username: username, Password: password, Policy: policy, Reset: true,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	args := append([]any{"ACL", "SETUSER", username}, stringsToAny(rules)...)
	if err := client.Do(r.Context(), args...).Err(); err != nil {
		http.Error(w, "Failed to create Redis ACL user", http.StatusInternalServerError)
		return
	}

	record := aclUserRecord{
		Username: username,
		Tenant:   request.Tenant,
		Profile:  policy.Name,
	}
	if err := writeACLUserRecord(r.Context(), record); err != nil {
		_, _ = client.Do(r.Context(), "ACL", "DELUSER", username).Result()
		http.Error(w, "Failed to persist Logma user metadata", http.StatusInternalServerError)
		return
	}

	maybeSaveACL(r.Context())

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(aclUserResponse{
		Username: username,
		Tenant:   request.Tenant,
		Profile:  policy.Name,
		Password: password,
	})
}

func replaceACLUserHandler(w http.ResponseWriter, r *http.Request) {
	username := strings.TrimSpace(chi.URLParam(r, "username"))
	if username == "" || username == func() string {
		p, _ := authProviderFromEnv()
		if p == nil {
			return ""
		}
		return p.AdminUser()
	}() {
		http.Error(w, "Invalid user", http.StatusBadRequest)
		return
	}

	var request aclUserRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}
	request.Tenant = strings.TrimSpace(request.Tenant)
	if err := auth.ValidateIdentifier(request.Tenant); err != nil {
		http.Error(w, "Invalid tenant: "+err.Error(), http.StatusBadRequest)
		return
	}
	provider, providerErr := authProviderFromEnv()
	if providerErr != nil || provider == nil {
		http.Error(w, "Auth provider unavailable", http.StatusInternalServerError)
		return
	}
	policy, err := provider.Policy(request.Profile)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	password := request.Password
	if password == "" {
		password, err = generateACLPassword()
		if err != nil {
			http.Error(w, "Failed to generate replacement password", http.StatusInternalServerError)
			return
		}
	}

	rules, err := provider.Rules(auth.UserSpec{
		Tenant: request.Tenant, Username: username, Password: password, Policy: policy, Reset: true,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	args := append([]any{"ACL", "SETUSER", username}, stringsToAny(rules)...)
	if err := client.Do(r.Context(), args...).Err(); err != nil {
		http.Error(w, "Failed to update Redis ACL user", http.StatusInternalServerError)
		return
	}

	record := aclUserRecord{
		Username: username,
		Tenant:   request.Tenant,
		Profile:  policy.Name,
	}
	if err := writeACLUserRecord(r.Context(), record); err != nil {
		http.Error(w, "Redis ACL updated but metadata update failed", http.StatusInternalServerError)
		return
	}

	maybeSaveACL(r.Context())

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(aclUserResponse{
		Username: username,
		Tenant:   request.Tenant,
		Profile:  policy.Name,
		Password: password,
	})
}

func deleteACLUserHandler(w http.ResponseWriter, r *http.Request) {
	username := strings.TrimSpace(chi.URLParam(r, "username"))
	if username == "" || username == func() string {
		p, _ := authProviderFromEnv()
		if p == nil {
			return ""
		}
		return p.AdminUser()
	}() {
		http.Error(w, "Invalid user", http.StatusBadRequest)
		return
	}
	if err := client.Do(r.Context(), "ACL", "DELUSER", username).Err(); err != nil {
		http.Error(w, "Failed to delete Redis ACL user", http.StatusInternalServerError)
		return
	}
	_ = client.Del(r.Context(), aclUserMetadataKey(username)).Err()
	maybeSaveACL(r.Context())
	w.WriteHeader(http.StatusNoContent)
}

func listACLUsersHandler(w http.ResponseWriter, r *http.Request) {
	keys, err := scanKeys(aclUserMetadataPrefix + "*")
	if err != nil {
		http.Error(w, "Failed to list Logma users", http.StatusInternalServerError)
		return
	}

	users := make([]aclUserRecord, 0, len(keys))
	for _, key := range keys {
		username := strings.TrimPrefix(key, aclUserMetadataPrefix)
		record, err := readACLUserRecord(r.Context(), username)
		if err == nil {
			users = append(users, record)
		}
	}
	sort.Slice(users, func(i, j int) bool {
		return users[i].Username < users[j].Username
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(users)
}

func stringsToAny(values []string) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

func maybeSaveACL(ctx context.Context) {
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("LOGMA_ACL_SAVE")), "true") {
		return
	}
	if err := client.Do(ctx, "ACL", "SAVE").Err(); err != nil {
		fmt.Printf("ACL SAVE failed: %v\n", err)
	}
}
