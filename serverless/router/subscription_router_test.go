package router

import (
 "net/http"
 "net/http/httptest"
 "testing"

 "github.com/redis/go-redis/v9"
)

func TestSubscriptionRouterRejectsHTTPAuthorityAndForeignChannelBeforeRuntime(t *testing.T) {
 client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
 defer client.Close()
 handler, err := NewSubscriptionRouter(client, []string{"scope:logma:lifecycle:probot-runtime:delivery.received"}, "operator")
 if err != nil { t.Fatal(err) }
 for _, tc := range []struct { token, channel string; status int }{
  {"", "", http.StatusUnauthorized},
  {"Bearer operator", "foreign:channel", http.StatusForbidden},
 } {
  req := httptest.NewRequest(http.MethodGet, "/events?channel="+tc.channel, nil)
  req.Header.Set("Authorization", tc.token)
  response := httptest.NewRecorder()
  handler.ServeHTTP(response, req)
  if response.Code != tc.status { t.Fatalf("status=%d, want %d", response.Code, tc.status) }
 }
}

func TestSubscriptionRuntimeKeepsOwnedClientDespiteRequestCredentialHeaders(t *testing.T) {
 client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
 defer client.Close()
 rt := NewSubscriptionRuntime(client, []string{"scope:logma:lifecycle:probot-runtime:delivery.received"})
 req := httptest.NewRequest(http.MethodGet, "/events", nil)
 req.Header.Set("X-Redis-Uri", "foreign:6379")
 rt.RecordInvocation(req, "request")
 if rt.Client != client { t.Fatal("request replaced composition-owned client") }
 if !rt.subscribeOnly || rt.GlobalRelayEnabled { t.Fatal("subscriber inherited control-plane authority") }
}
