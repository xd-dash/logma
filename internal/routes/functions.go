package routes

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/redis/go-redis/v9"
)

var aclTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_.:@-]+$`)

type registerFunctionRequest struct {
	Source        string   `json:"source"`
	FunctionNames []string `json:"functionNames"`
	ACLUsername   string   `json:"aclUsername"`
	ChannelPattern string  `json:"channelPattern,omitempty"`
}

func registerFunctionHandler(w http.ResponseWriter, r *http.Request) {
	tenantID, err := tenantIDFromRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var body registerFunctionRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}
	body.Source = strings.TrimSpace(body.Source)
	body.ACLUsername = strings.TrimSpace(body.ACLUsername)
	body.ChannelPattern = strings.TrimSpace(body.ChannelPattern)
	if body.Source == "" || !strings.HasPrefix(body.Source, "#!lua name=") {
		http.Error(w, "source must be a Redis Lua function library beginning with '#!lua name='", http.StatusBadRequest)
		return
	}
	if len(body.FunctionNames) == 0 {
		http.Error(w, "functionNames must not be empty", http.StatusBadRequest)
		return
	}
	if body.ACLUsername == "" {
		body.ACLUsername = tenantID
	}
	if !aclTokenPattern.MatchString(body.ACLUsername) {
		http.Error(w, "aclUsername contains unsupported characters", http.StatusBadRequest)
		return
	}
	for _, name := range body.FunctionNames {
		if !aclTokenPattern.MatchString(name) {
			http.Error(w, "functionNames contains an invalid function name", http.StatusBadRequest)
			return
		}
	}
	if body.ChannelPattern == "" {
		body.ChannelPattern = tenantID + ":*"
	}
	if strings.ContainsAny(body.ChannelPattern, " \t\r\n") {
		http.Error(w, "channelPattern must not contain whitespace", http.StatusBadRequest)
		return
	}

	admin, err := newAdminRedisClient()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer admin.Close()

	loaded, err := admin.Do(r.Context(), "FUNCTION", "LOAD", "REPLACE", body.Source).Result()
	if err != nil {
		http.Error(w, "Failed to load Redis function library: "+err.Error(), http.StatusBadGateway)
		return
	}

	rules := []interface{}{
		"ACL", "SETUSER", body.ACLUsername,
		"on",
		"resetkeys",
		"resetchannels",
		"-@all",
		"+ping",
		"+client|setinfo",
		"+get",
		"+set",
		"+del",
		"+scan",
		"+subscribe",
		"+unsubscribe",
		"+psubscribe",
		"+punsubscribe",
		"+pubsub|channels",
		"~" + activeSubscriptionPattern(tenantID, "*", "*"),
		"~" + subscriptionGroupPattern(tenantID, "*", "*", "*"),
		"&" + body.ChannelPattern,
	}
	for _, name := range body.FunctionNames {
		rules = append(rules, "+fcall|"+name, "+fcall_ro|"+name)
	}

	if err := admin.Do(r.Context(), rules...).Err(); err != nil {
		http.Error(w, "Function loaded but ACL update failed: "+err.Error(), http.StatusBadGateway)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"tenantID":       tenantID,
		"library":        fmt.Sprint(loaded),
		"functionNames":  body.FunctionNames,
		"aclUsername":    body.ACLUsername,
		"keyPatterns": []string{
			activeSubscriptionPattern(tenantID, "*", "*"),
			subscriptionGroupPattern(tenantID, "*", "*", "*"),
		},
		"channelPattern": body.ChannelPattern,
	})
}

func newAdminRedisClient() (*redis.Client, error) {
	username := os.Getenv("REDIS_ADMIN_USERNAME")
	password := os.Getenv("REDIS_ADMIN_PASSWORD")
	if username == "" && password == "" {
		return nil, errors.New("REDIS_ADMIN_USERNAME/REDIS_ADMIN_PASSWORD are required for function registration")
	}
	return redis.NewClient(&redis.Options{
		Addr:         os.Getenv("REDIS_URI"),
		Username:     username,
		Password:     password,
		DB:           0,
		PoolSize:     1,
		MinIdleConns: 0,
		MaxIdleConns: 1,
	}), nil
}
