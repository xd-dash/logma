package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestScopedLegacyRedisKeys(t *testing.T) {
	t.Setenv("FATLINE_SCOPE", "fatline-prod")
	key := generateTempChannelID("fatline-prod:logma:shutdown")
	if len(key) < len("fatline-prod:active_subscriptions:") || key[:len("fatline-prod:active_subscriptions:")] != "fatline-prod:active_subscriptions:" {
		t.Fatalf("generated key is not scoped: %q", key)
	}
	if got := extractSubscriptionID(key); got == "" {
		t.Fatalf("could not extract scoped subscription ID from %q", key)
	}
	if _, ok := extractGroupChannelName("fatline-prod:subscription_groups:g:1:shutdown", "g"); !ok {
		t.Fatal("could not parse scoped subscription group key")
	}
	if _, ok := extractGroupChannelName("other:subscription_groups:g:1:shutdown", "g"); ok {
		t.Fatal("accepted a key from another scope")
	}
}

func TestScopedPubSubChannel(t *testing.T) {
	t.Setenv("FATLINE_SCOPE", "fatline-prod")
	if got := scopedRedisChannel("logma:shutdown"); got != "fatline-prod:logma:shutdown" {
		t.Fatalf("scopedRedisChannel() = %q", got)
	}
	if got := scopedRedisChannel("fatline-prod:logma:shutdown"); got != "fatline-prod:logma:shutdown" {
		t.Fatalf("already scoped channel changed: %q", got)
	}
}

func TestInvalidFatlineScopeFailsClosed(t *testing.T) {
	t.Setenv("FATLINE_SCOPE", "fatline-prod:*")
	called := false
	handler := validateFatlineScope(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/channels/", nil))
	if called || recorder.Code != http.StatusInternalServerError {
		t.Fatalf("invalid scope passed middleware: called=%v status=%d", called, recorder.Code)
	}
}
