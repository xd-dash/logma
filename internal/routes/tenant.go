package routes

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

const (
	tenantIDHeader      = "X-Tenant-ID"
	redisUsernameHeader = "X-Redis-Username"
	redisPasswordHeader = "X-Redis-Password"
)

type tenantScope struct {
	ID     string
	Client *redis.Client
}

type tenantClientRegistry struct {
	mu      sync.Mutex
	clients map[string]*redis.Client
}

func newTenantClientRegistry() *tenantClientRegistry {
	return &tenantClientRegistry{clients: make(map[string]*redis.Client)}
}

func (r *tenantClientRegistry) client(username, password string) *redis.Client {
	digest := sha256.Sum256([]byte(username + "\x00" + password))
	key := hex.EncodeToString(digest[:])

	r.mu.Lock()
	defer r.mu.Unlock()

	if client, ok := r.clients[key]; ok {
		return client
	}

	poolSize := 2
	if value := os.Getenv("LOGMA_REDIS_POOL_SIZE"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			poolSize = parsed
		}
	}

	client := redis.NewClient(&redis.Options{
		Addr:         os.Getenv("REDIS_URI"),
		Username:     username,
		Password:     password,
		DB:           0,
		PoolSize:     poolSize,
		MinIdleConns: 0,
		MaxIdleConns: 1,
	})
	r.clients[key] = client
	return client
}

func (r *tenantClientRegistry) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var joined error
	for key, client := range r.clients {
		if err := client.Close(); err != nil {
			joined = errors.Join(joined, err)
		}
		delete(r.clients, key)
	}
	return joined
}

func tenantIDFromRequest(r *http.Request) (string, error) {
	tenantID := strings.TrimSpace(r.Header.Get(tenantIDHeader))
	if tenantID == "" {
		tenantID = strings.TrimSpace(os.Getenv("LOGMA_DEFAULT_TENANT_ID"))
	}
	if tenantID == "" {
		return "", errors.New("tenant ID is required")
	}
	if strings.Contains(tenantID, ":") {
		return "", errors.New("tenant ID must not contain ':'")
	}
	return tenantID, nil
}

func tenantFromRequest(r *http.Request) (tenantScope, error) {
	tenantID, err := tenantIDFromRequest(r)
	if err != nil {
		return tenantScope{}, err
	}

	username := strings.TrimSpace(r.Header.Get(redisUsernameHeader))
	password := r.Header.Get(redisPasswordHeader)
	if username == "" {
		username = os.Getenv("REDIS_USERNAME")
	}
	if password == "" {
		password = os.Getenv("REDISCLI_AUTH")
	}
	if username == "" && password == "" {
		return tenantScope{}, errors.New("Redis ACL credentials are required")
	}

	return tenantScope{
		ID:     tenantID,
		Client: tenantClients.client(username, password),
	}, nil
}

func defaultTenantScope() (tenantScope, error) {
	tenantID := strings.TrimSpace(os.Getenv("LOGMA_DEFAULT_TENANT_ID"))
	if tenantID == "" {
		return tenantScope{}, errors.New("LOGMA_DEFAULT_TENANT_ID is required")
	}
	username := os.Getenv("REDIS_USERNAME")
	password := os.Getenv("REDISCLI_AUTH")
	if username == "" && password == "" {
		return tenantScope{}, errors.New("default Redis ACL credentials are required")
	}
	return tenantScope{ID: tenantID, Client: tenantClients.client(username, password)}, nil
}

func activeSubscriptionPattern(tenantID, subscriptionID, channel string) string {
	return "as:" + tenantID + ":" + subscriptionID + ":" + channel
}

func subscriptionGroupPattern(tenantID, groupID, subscriptionID, channel string) string {
	prefix := "sg:" + tenantID + ":" + groupID + ":"
	if subscriptionID == "" && channel == "" {
		return prefix
	}
	return prefix + subscriptionID + ":" + channel
}
