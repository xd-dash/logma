package routes

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestVersionedPubSubRoutes(t *testing.T) {
	t.Parallel()

	router, ok := NewRouter().(chi.Routes)
	if !ok {
		t.Fatal("router does not implement chi.Routes")
	}

	got := map[string]bool{}
	if err := chi.Walk(router, func(
		method string,
		route string,
		_ http.Handler,
		_ ...func(http.Handler) http.Handler,
	) error {
		got[method+" "+route] = true
		return nil
	}); err != nil {
		t.Fatalf("walk routes: %v", err)
	}

	want := []string{
		"GET /pubsub/api/v0.0.1/channels/",
		"POST /pubsub/api/v0.0.1/channels/",
		"GET /pubsub/api/v0.0.1/channels/groups",
		"POST /pubsub/api/v0.0.1/channels/groups",
		"GET /pubsub/api/v0.0.1/channels/groups/{groupID}",
		"DELETE /pubsub/api/v0.0.1/channels/groups/{groupID}",
		"POST /pubsub/api/v0.0.1/channels/groups/{groupID}/load",
		"POST /pubsub/api/v0.0.1/channels/{channelName}",
		"GET /pubsub/api/v0.0.1/channels/{channelName}/subscribe",
		"POST /pubsub/api/v0.0.1/channels/{channelName}/subscribe",
	}

	for _, route := range want {
		if !got[route] {
			t.Errorf("missing route %s", route)
		}
	}
}

func TestCallbackURLFromRequestQueryAliases(t *testing.T) {
	t.Parallel()

	for _, query := range []string{
		"callbackURL=https%3A%2F%2Fexample.com%2Fone",
		"callback=https%3A%2F%2Fexample.com%2Ftwo",
		"callback_url=https%3A%2F%2Fexample.com%2Fthree",
	} {
		req := httptest.NewRequest(
			http.MethodGet,
			"/?"+query,
			nil,
		)

		got, err := callbackURLFromRequest(req)
		if err != nil {
			t.Fatalf("callbackURLFromRequest(%q): %v", query, err)
		}
		if !strings.HasPrefix(got, "https://example.com/") {
			t.Fatalf("callbackURLFromRequest(%q) = %q", query, got)
		}
	}
}

func TestCallbackURLFromRequestJSON(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader(
			`{"callbackURL":"https://example.com/callback"}`,
		),
	)

	got, err := callbackURLFromRequest(req)
	if err != nil {
		t.Fatalf("callbackURLFromRequest: %v", err)
	}
	if got != "https://example.com/callback" {
		t.Fatalf("callback URL = %q", got)
	}
}

func TestGenerateChannelName(t *testing.T) {
	t.Parallel()

	first, err := generateChannelName()
	if err != nil {
		t.Fatalf("generate first channel name: %v", err)
	}
	second, err := generateChannelName()
	if err != nil {
		t.Fatalf("generate second channel name: %v", err)
	}

	if !strings.HasPrefix(first, "channel-") {
		t.Fatalf("channel name %q lacks channel- prefix", first)
	}
	if first == second {
		t.Fatalf("generated duplicate channel names %q", first)
	}
}
