package routes

import (
	"encoding/json"
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
		"GET /pubsub/api/v0.0.1/auth/profile",
		"GET /pubsub/api/v0.0.1/users/",
		"POST /pubsub/api/v0.0.1/users/",
		"PUT /pubsub/api/v0.0.1/users/{username}",
		"DELETE /pubsub/api/v0.0.1/users/{username}",
		"GET /pubsub/api/v0.0.1/functions/",
		"POST /pubsub/api/v0.0.1/functions/",
		"DELETE /pubsub/api/v0.0.1/functions/{name}",
		"GET /pubsub/api/v0.0.1/channels/",
		"POST /pubsub/api/v0.0.1/channels/",
		"POST /pubsub/api/v0.0.1/channels/save",
		"GET /pubsub/api/v0.0.1/channels/groups",
		"POST /pubsub/api/v0.0.1/channels/groups",
		"GET /pubsub/api/v0.0.1/channels/groups/{groupID}",
		"DELETE /pubsub/api/v0.0.1/channels/groups/{groupID}",
		"POST /pubsub/api/v0.0.1/channels/groups/{groupID}/load",
		"POST /pubsub/api/v0.0.1/channels/{channelName}",
		"POST /pubsub/api/v0.0.1/channels/{channelName}/publish",
		"GET /pubsub/api/v0.0.1/channels/{channelName}/subscribe",
		"POST /pubsub/api/v0.0.1/channels/{channelName}/subscribe",
	}

	for _, route := range want {
		if !got[route] {
			t.Errorf("missing route %s", route)
		}
	}
}

func TestCallbackConfigFromQuerySupportsRepeatedURLs(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(
		http.MethodGet,
		"/?callbackURL=https%3A%2F%2Fexample.com%2Fone&callbackURL=https%3A%2F%2Fexample.com%2Ftwo",
		nil,
	)

	config, err := parseCallbackConfigFromRequest(req)
	if err != nil {
		t.Fatalf("parse callback config: %v", err)
	}
	if len(config.Callbacks) != 2 {
		t.Fatalf("callbacks = %d, want 2", len(config.Callbacks))
	}
}

func TestCallbackConfigFromJSONSupportsStringAndList(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		`{"callbackURL":"https://example.com/one"}`,
		`{"callbackURL":["https://example.com/one","https://example.com/two"]}`,
		`{"callbackURLs":["https://example.com/one","https://example.com/two"]}`,
	} {
		req := httptest.NewRequest(
			http.MethodPost,
			"/",
			strings.NewReader(body),
		)

		config, err := parseCallbackConfigFromRequest(req)
		if err != nil {
			t.Fatalf("parse callback config %s: %v", body, err)
		}
		if len(config.Callbacks) == 0 {
			t.Fatalf("parse callback config %s returned no callbacks", body)
		}
	}
}

func TestCallbackConfigFromJSONSupportsOneOrManySchemes(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		`{"callbacks":{"type":"http","url":"https://example.com/one"}}`,
		`{"callbacks":[{"type":"http","url":"https://example.com/one"},{"type":"http","urls":["https://example.com/two","https://example.com/three"]}]}`,
		`{"callbacks":{"type":"custom","config":{"topic":"example"}}}`,
	} {
		req := httptest.NewRequest(
			http.MethodPost,
			"/",
			strings.NewReader(body),
		)

		config, err := parseCallbackConfigFromRequest(req)
		if err != nil {
			t.Fatalf("parse callback config %s: %v", body, err)
		}
		if len(config.Callbacks) == 0 {
			t.Fatalf("parse callback config %s returned no callbacks", body)
		}
	}
}

func TestStoredCallbackConfigBackwardsCompatibility(t *testing.T) {
	t.Parallel()

	legacy, err := decodeStoredCallbackConfig(
		"https://example.com/callback",
	)
	if err != nil {
		t.Fatalf("decode legacy callback: %v", err)
	}
	if len(legacy.Callbacks) != 1 ||
		legacy.Callbacks[0].URL != "https://example.com/callback" {
		t.Fatalf("unexpected legacy callback: %#v", legacy)
	}

	rich := callbackConfig{
		Version: callbackConfigVersion,
		Callbacks: []callbackScheme{
			{
				Type: "http",
				URLs: []string{
					"https://example.com/one",
					"https://example.com/two",
				},
			},
			{
				Type:   "custom",
				Config: json.RawMessage(`{"topic":"example"}`),
			},
		},
	}

	stored, err := encodeStoredCallbackConfig(rich)
	if err != nil {
		t.Fatalf("encode rich callback: %v", err)
	}
	if !strings.HasPrefix(stored, "{") {
		t.Fatalf("rich callback was not stored as schema JSON: %q", stored)
	}

	decoded, err := decodeStoredCallbackConfig(stored)
	if err != nil {
		t.Fatalf("decode rich callback: %v", err)
	}
	if len(decoded.Callbacks) != 2 {
		t.Fatalf("decoded callbacks = %d, want 2", len(decoded.Callbacks))
	}
}

func TestOneOrManyRaw(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		`{"channels":{"channel":"one","callbackURL":"https://example.com/one"}}`,
		`{"channels":[{"channel":"one","callbackURL":"https://example.com/one"},"1234"]}`,
	} {
		var request groupCreateRequest
		if err := json.Unmarshal([]byte(body), &request); err != nil {
			t.Fatalf("unmarshal %s: %v", body, err)
		}
		if len(request.Channels) == 0 {
			t.Fatalf("unmarshal %s produced no channels", body)
		}
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
