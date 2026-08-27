package routes

import "testing"

func TestMaraiStateDefaults(t *testing.T) {
	t.Setenv("MARAI_CACHE_DB", "")
	t.Setenv("MARAI_KMS_KEY_ID", "")
	t.Setenv("MARAI_CACHE_NAMESPACE", "")

	store := newMaraiStateStore(nil)
	if store.db != 1 || store.keyID != "logma" || store.namespace != "logma" {
		t.Fatalf("unexpected defaults: db=%d keyID=%q namespace=%q", store.db, store.keyID, store.namespace)
	}
}

func TestMaraiStateRejectsReservedDBZero(t *testing.T) {
	t.Setenv("MARAI_CACHE_DB", "0")
	store := newMaraiStateStore(nil)
	if store.db != 1 {
		t.Fatalf("db=%d, want fallback to 1", store.db)
	}
}

func TestMaraiStateAcceptsConfiguredSecretDB(t *testing.T) {
	t.Setenv("MARAI_CACHE_DB", "8")
	t.Setenv("MARAI_KMS_KEY_ID", "tenant-logma")
	t.Setenv("MARAI_CACHE_NAMESPACE", "callbacks")

	store := newMaraiStateStore(nil)
	if store.db != 8 || store.keyID != "tenant-logma" || store.namespace != "callbacks" {
		t.Fatalf("unexpected configuration: db=%d keyID=%q namespace=%q", store.db, store.keyID, store.namespace)
	}
}

func TestNormalizeCallbackDefaultsBearer(t *testing.T) {
	secret, err := normalizeCallback(callbackSecret{
		URL:         " https://example.test/callback ",
		AccessToken: "token",
	})
	if err != nil {
		t.Fatal(err)
	}
	if secret.URL != "https://example.test/callback" || secret.TokenScheme != "Bearer" {
		t.Fatalf("unexpected callback: %+v", secret)
	}
}

func TestNormalizeCallbackRejectsHeaderInjection(t *testing.T) {
	_, err := normalizeCallback(callbackSecret{
		URL:         "https://example.test/callback",
		AccessToken: "token",
		TokenScheme: "Bearer\r\nX-Evil: yes",
	})
	if err == nil {
		t.Fatal("expected token scheme validation error")
	}
}
