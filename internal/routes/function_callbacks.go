package routes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
	logmaacl "github.com/xd-dash/logma/acl"
)

type redisFunctionCallbackConfig struct {
	Name string   `json:"name"`
	Keys []string `json:"keys,omitempty"`
	Args []string `json:"args,omitempty"`
}

func canonicalTenantKey(tenant, key string) string {
	key = strings.TrimSpace(key)
	return logmaacl.TenantKeyPrefix(tenant) + key
}

func dispatchRedisFunctionCallback(
	dispatcher *callbackDispatcher,
	callback callbackScheme,
	message PublishRequest,
	tenant string,
	redisClient *redis.Client,
) {
	if tenant == "" || redisClient == nil {
		fmt.Printf("redis-function callback skipped: tenant Redis identity is unavailable\n")
		return
	}

	var cfg redisFunctionCallbackConfig
	if err := json.Unmarshal(callback.Config, &cfg); err != nil {
		fmt.Printf("redis-function callback config error: %v\n", err)
		return
	}

	functionName, err := logmaacl.TenantFunctionName(tenant, strings.TrimSpace(cfg.Name))
	if err != nil {
		fmt.Printf("redis-function callback name error: %v\n", err)
		return
	}

	keys := make([]string, 0, len(cfg.Keys))
	for _, key := range cfg.Keys {
		keys = append(keys, canonicalTenantKey(tenant, key))
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		fmt.Printf("redis-function callback message error: %v\n", err)
		return
	}

	args := append([]string(nil), cfg.Args...)
	args = append(args, string(messageJSON))

	dispatcher.dispatch(
		func(_ string, _ PublishRequest) {
			callArgs := make([]any, 0, 3+len(keys)+len(args))
			callArgs = append(callArgs, "FCALL", functionName, len(keys))
			for _, key := range keys {
				callArgs = append(callArgs, key)
			}
			for _, arg := range args {
				callArgs = append(callArgs, arg)
			}

			ctx, cancel := context.WithTimeout(context.Background(), callbackTimeout)
			defer cancel()
			if err := redisClient.Do(ctx, callArgs...).Err(); err != nil {
				fmt.Printf(
					"redis-function callback %s failed for tenant %s: %v\n",
					functionName,
					tenant,
					err,
				)
			}
		},
		functionName,
		message,
	)
}
