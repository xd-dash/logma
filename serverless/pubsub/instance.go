package pubsub

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"sync"
)

var InstanceID = sync.OnceValue(computeInstanceID)

func computeInstanceID() string {
	if os.Getenv("K_SERVICE") != "" {
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			return hostname
		}
	}
	return "dev-" + randomHex(8)
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "static"
	}
	return hex.EncodeToString(buf)
}
