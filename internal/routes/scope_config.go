package routes

import (
	"os"
	"strconv"
)

const (
	defaultScopeID        = "default"
	defaultScopeNamespace = "default"
	defaultMaraiKeyID     = "logma"
)

type scopeConfig struct {
	ID              string
	Namespace       string
	DB              int
	MaraiKeyID      string
	Provider        string
	ExternalProject string
}

func scopeConfigFromEnv() scopeConfig {
	db := 0
	dbRaw := os.Getenv("LOGMA_SCOPE_DB")
	if dbRaw == "" {
		dbRaw = os.Getenv("REDIS_DB")
	}
	if parsed, err := strconv.Atoi(dbRaw); err == nil && parsed >= 0 {
		db = parsed
	}

	scopeID := os.Getenv("LOGMA_SCOPE_ID")
	if scopeID == "" {
		scopeID = defaultScopeID
	}
	namespace := os.Getenv("LOGMA_SCOPE_NAMESPACE")
	if namespace == "" {
		namespace = defaultScopeNamespace
	}
	keyID := os.Getenv("MARAI_KEY_ID")
	if keyID == "" {
		keyID = defaultMaraiKeyID
	}

	return scopeConfig{
		ID:              scopeID,
		Namespace:       namespace,
		DB:              db,
		MaraiKeyID:      keyID,
		Provider:        os.Getenv("LOGMA_SCOPE_PROVIDER"),
		ExternalProject: os.Getenv("LOGMA_EXTERNAL_PROJECT"),
	}
}

func (s scopeConfig) maraiEncryptArgs(plaintext []byte) []any {
	return []any{
		"FCALL", "kms_encrypt", 0,
		s.ID, s.MaraiKeyID, s.Namespace, plaintext,
	}
}

func (s scopeConfig) maraiDecryptArgs(envelope []byte) []any {
	return []any{
		"FCALL", "kms_decrypt", 0,
		s.ID, s.MaraiKeyID, s.Namespace, envelope,
	}
}
