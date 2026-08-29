package routes

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	logmaacl "github.com/xd-dash/logma/acl"
	legacyprofile "github.com/xd-dash/logma/acl/profile/legacy"
	managedprofile "github.com/xd-dash/logma/acl/profile/managed"
	"github.com/redis/go-redis/v9"
)

type principalContextKey struct{}

type requestPrincipal struct {
	Username string
	Tenant   string
	Admin    bool
	Policy   logmaacl.TenantPolicy
	Password string
}

func authProfileFromEnv() logmaacl.Profile {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOGMA_AUTH_PROFILE"))) {
	case "", "legacy":
		return legacyprofile.New()
	case "managed", "acl", "multi-tenant":
		return managedprofile.New(strings.TrimSpace(os.Getenv("LOGMA_ADMIN_USER")))
	default:
		return logmaacl.Profile{Name: "invalid"}
	}
}

func principalFromRequest(r *http.Request) (requestPrincipal, bool) {
	value := r.Context().Value(principalContextKey{})
	principal, ok := value.(requestPrincipal)
	return principal, ok
}

func authenticateRequest(next http.Handler) http.Handler {
	profile := authProfileFromEnv()
	if profile.Name == "legacy" {
		return authenticateAPIKey(next)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if profile.Name != "managed" {
			http.Error(w, "Invalid LOGMA_AUTH_PROFILE", http.StatusInternalServerError)
			return
		}

		username, password, ok := r.BasicAuth()
		username = strings.TrimSpace(username)
		if !ok || username == "" || password == "" {
			w.Header().Set("WWW-Authenticate", "Basic realm=\"logma\"")
			http.Error(w, "Redis ACL credentials required", http.StatusUnauthorized)
			return
		}

		authClient := redis.NewClient(redisOptionsForCredentials(username, password))
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		err := authClient.Ping(ctx).Err()
		cancel()
		_ = authClient.Close()
		if err != nil {
			http.Error(w, "Invalid Redis ACL credentials", http.StatusUnauthorized)
			return
		}

		principal := requestPrincipal{
			Username: username,
			Password: password,
			Admin:    username == profile.AdminUser,
		}

		if !principal.Admin {
			record, err := readACLUserRecord(r.Context(), username)
			if err != nil {
				http.Error(w, "Redis user is not managed by Logma", http.StatusForbidden)
				return
			}
			principal.Tenant = record.Tenant
			policy, err := logmaacl.PolicyByName(record.Profile)
			if err != nil {
				http.Error(w, "Invalid stored ACL profile", http.StatusInternalServerError)
				return
			}
			principal.Policy = policy

			// The unversioned API predates tenant scoping and executes through
			// the service control-plane client. Do not expose it to tenants.
			if strings.HasPrefix(r.URL.Path, "/channels") ||
				r.URL.Path == "/bootstrap" {
				http.Error(w, "Legacy API is application-admin only in managed mode", http.StatusForbidden)
				return
			}
		}

		ctxWithPrincipal := context.WithValue(r.Context(), principalContextKey{}, principal)
		next.ServeHTTP(w, r.WithContext(ctxWithPrincipal))
	})
}

func redisOptionsForCredentials(username, password string) *redis.Options {
	options := redisOptionsFromEnv()
	copy := *options
	copy.Username = username
	copy.Password = password
	return &copy
}

func requireApplicationAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		profile := authProfileFromEnv()
		if profile.Name != "managed" {
			http.Error(w, "User management requires managed auth profile", http.StatusNotFound)
			return
		}
		principal, ok := principalFromRequest(r)
		if !ok || !principal.Admin {
			http.Error(w, "Application admin required", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func authProfileHandler(w http.ResponseWriter, r *http.Request) {
	profile := authProfileFromEnv()
	response := map[string]any{
		"profile": profile.Name,
		"managed": profile.Managed,
	}
	if profile.Managed {
		response["adminUser"] = profile.AdminUser
		response["tenantProfiles"] = []string{
			"tenant",
			"tenant-functions",
			"publisher",
			"subscriber",
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}
