package routes

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/dash-xd/ratelimiter/auth"
	managedprofile "github.com/dash-xd/ratelimiter/auth/profile/managed"
	"github.com/redis/go-redis/v9"
)

type principalContextKey struct{}

type requestPrincipal struct {
	Username string
	Tenant   string
	Admin    bool
	Policy   auth.Policy
	Password string
}

func authProviderFromEnv() (auth.Provider, error) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOGMA_AUTH_PROVIDER"))) {
	case "", "default", "legacy", "rediscliauth":
		return nil, nil
	case "managed", "ratelimiter-managed", "redis-acl":
		return managedprofile.New(managedprofile.Config{
			AdminUser:      strings.TrimSpace(os.Getenv("LOGMA_ADMIN_USER")),
			UsernamePrefix: "logma-tenant-",
			KeyPrefix:      "logma:tenant:",
			ChannelPrefix:  "tenant:",
			FunctionPrefix: "logma_",
		})
	default:
		return nil, &invalidAuthProviderError{name: os.Getenv("LOGMA_AUTH_PROVIDER")}
	}
}

type invalidAuthProviderError struct {
	name string
}

func (e *invalidAuthProviderError) Error() string {
	return "invalid LOGMA_AUTH_PROVIDER " + e.name
}

func principalFromRequest(r *http.Request) (requestPrincipal, bool) {
	value := r.Context().Value(principalContextKey{})
	principal, ok := value.(requestPrincipal)
	return principal, ok
}

func authenticateRequest(next http.Handler) http.Handler {
	provider, err := authProviderFromEnv()
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		})
	}
	if provider == nil {
		return authenticateAPIKey(next)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		username = strings.TrimSpace(username)
		if !ok || username == "" || password == "" {
			w.Header().Set("WWW-Authenticate", "Basic realm=\"logma\"")
			http.Error(w, "Redis ACL credentials required", http.StatusUnauthorized)
			return
		}

		authClient := redis.NewClient(redisOptionsForCredentials(username, password))
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		pingErr := authClient.Ping(ctx).Err()
		cancel()
		_ = authClient.Close()
		if pingErr != nil {
			http.Error(w, "Invalid Redis ACL credentials", http.StatusUnauthorized)
			return
		}

		principal := requestPrincipal{
			Username: username,
			Password: password,
			Admin:    username == provider.AdminUser(),
		}

		if !principal.Admin {
			record, err := readACLUserRecord(r.Context(), username)
			if err != nil {
				http.Error(w, "Redis user is not managed by Logma", http.StatusForbidden)
				return
			}
			principal.Tenant = record.Tenant
			policy, err := provider.Policy(record.Profile)
			if err != nil {
				http.Error(w, "Invalid stored ACL profile", http.StatusInternalServerError)
				return
			}
			principal.Policy = policy

			if strings.HasPrefix(r.URL.Path, "/channels") ||
				r.URL.Path == "/bootstrap" {
				http.Error(w, "Legacy API is application-admin only with an auth provider", http.StatusForbidden)
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
		provider, err := authProviderFromEnv()
		if err != nil || provider == nil {
			http.Error(w, "User management requires an auth provider", http.StatusNotFound)
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
	provider, err := authProviderFromEnv()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]any{
		"provider": "default",
		"managed":  false,
	}
	if provider != nil {
		response["provider"] = provider.Name()
		response["managed"] = true
		response["adminUser"] = provider.AdminUser()
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
