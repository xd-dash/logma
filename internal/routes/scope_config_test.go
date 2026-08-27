package routes

import (
	"reflect"
	"testing"
)

func TestScopeConfigDefaults(t *testing.T) {
	for _, key := range []string{
		"LOGMA_SCOPE_ID", "LOGMA_SCOPE_NAMESPACE", "LOGMA_SCOPE_DB",
		"LOGMA_SCOPE_PROVIDER", "LOGMA_EXTERNAL_PROJECT", "MARAI_KEY_ID", "REDIS_DB",
	} {
		t.Setenv(key, "")
	}

	got := scopeConfigFromEnv()
	if got.ID != defaultScopeID || got.Namespace != defaultScopeNamespace || got.DB != 0 || got.MaraiKeyID != defaultMaraiKeyID {
		t.Fatalf("unexpected defaults: %+v", got)
	}
}

func TestScopeConfigProviderMetadataIsOptional(t *testing.T) {
	t.Setenv("LOGMA_SCOPE_ID", "scope-7f85")
	t.Setenv("LOGMA_SCOPE_NAMESPACE", "subscriptions")
	t.Setenv("LOGMA_SCOPE_DB", "4")
	t.Setenv("LOGMA_SCOPE_PROVIDER", "gcp")
	t.Setenv("LOGMA_EXTERNAL_PROJECT", "customer-prod")
	t.Setenv("MARAI_KEY_ID", "logma")

	got := scopeConfigFromEnv()
	if got.ID != "scope-7f85" || got.Namespace != "subscriptions" || got.DB != 4 || got.Provider != "gcp" || got.ExternalProject != "customer-prod" {
		t.Fatalf("unexpected scope: %+v", got)
	}
}

func TestScopeDBOverridesLegacyRedisDB(t *testing.T) {
	t.Setenv("REDIS_DB", "2")
	t.Setenv("LOGMA_SCOPE_DB", "7")

	if got := redisOptionsFromEnv().DB; got != 7 {
		t.Fatalf("DB=%d, want 7", got)
	}
}

func TestMaraiScopedArgumentsExcludeProviderMetadata(t *testing.T) {
	scope := scopeConfig{
		ID:              "scope-7f85",
		Namespace:       "subscriptions",
		DB:              4,
		MaraiKeyID:      "logma",
		Provider:        "gcp",
		ExternalProject: "customer-prod",
	}

	wantEncrypt := []any{"FCALL", "kms_encrypt", 0, "scope-7f85", "logma", "subscriptions", []byte("secret")}
	if got := scope.maraiEncryptArgs([]byte("secret")); !reflect.DeepEqual(got, wantEncrypt) {
		t.Fatalf("encrypt args=%#v", got)
	}

	wantDecrypt := []any{"FCALL", "kms_decrypt", 0, "scope-7f85", "logma", "subscriptions", []byte("MRA2")}
	if got := scope.maraiDecryptArgs([]byte("MRA2")); !reflect.DeepEqual(got, wantDecrypt) {
		t.Fatalf("decrypt args=%#v", got)
	}
}
