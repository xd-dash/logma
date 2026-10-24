package routes

import (
 "github.com/redis/go-redis/v9"
 "github.com/xd-dash/logma/internal/redisconfig"
)

func redisOptionsFromEnv() *redis.Options { return redisconfig.OptionsFromEnv() }
