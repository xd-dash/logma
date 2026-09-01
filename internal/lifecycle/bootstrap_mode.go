package lifecycle

import (
	"fmt"
	"os"
	"strings"
)

const lifecycleBootstrapEnv = "LOGMA_RATELIMITER_BOOTSTRAP"

func lifecycleBootstrapInternal() (bool, error) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(lifecycleBootstrapEnv))) {
	case "", "external":
		return false, nil
	case "internal":
		return true, nil
	default:
		return false, fmt.Errorf("%s must be internal or external", lifecycleBootstrapEnv)
	}
}
