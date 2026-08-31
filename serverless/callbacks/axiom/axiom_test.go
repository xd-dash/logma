package axiom

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xd-dash/logma/serverless/pubsub"
)

func TestImportantChannel(t *testing.T) {
	for _, channel := range []string{
		"news:headline", "agent:signal:spy", "trade:execution:paper", "lifecycle.shutdown",
	} {
		if !importantChannel(channel) {
			t.Fatalf("expected %q to be important", channel)
		}
	}
	for _, channel := range []string{"stonks:quote:SPY", "stonks:trade:SPY", "stonks:bar:AAPL"} {
		if importantChannel(channel) {
			t.Fatalf("expected raw market channel %q to be filtered", channel)
		}
	}
}

func TestObserverStreamsCorrelatedEvent(t *testing.T) {
	var mu sync.Mutex
	var got []map[string]any
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("unexpected authorization header")
		}
		var body []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode: %v", err)
		}
		mu.Lock()
		got = append(got, body...)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	observer := New(Config{
		Enabled:     true,
		Token:       "test-token",
		Dataset:     DefaultDataset,
		Domain:      server.URL,
		PublishMode: PublishAll,
		Static: map[string]any{
			"fatline_id":    "fatline-test",
			"deployment_id": "deploy-123",
			"mode":          "smoke",
		},
	})
	observer.client = server.Client()

	observer.Observe(context.Background(), pubsub.ObservabilityEvent{
		Time:       time.Date(2026, 8, 31, 9, 0, 0, 0, time.UTC),
		Kind:       "fatline",
		Phase:      "publish",
		Status:     "published",
		Namespace:  "news",
		InstanceID: "instance-1",
		RequestID:  "request-1",
		Channel:    "news:headline",
		Payload:    json.RawMessage(`{"title":"example"}`),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := observer.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("got %d events, want 1", len(got))
	}
	if got[0]["fatline_id"] != "fatline-test" || got[0]["deployment_id"] != "deploy-123" {
		t.Fatalf("missing static correlation fields: %#v", got[0])
	}
	if got[0]["channel"] != "news:headline" {
		t.Fatalf("unexpected channel: %#v", got[0]["channel"])
	}
}

func TestImportantModeFiltersRawMarketPublishesButKeepsLifecycle(t *testing.T) {
	o := New(Config{PublishMode: PublishImportant})
	if o.keep(pubsub.ObservabilityEvent{Phase: "publish", Channel: "stonks:quote:SPY"}) {
		t.Fatal("raw quote should be filtered")
	}
	if !o.keep(pubsub.ObservabilityEvent{Phase: "publish", Channel: "agent:signal:SPY"}) {
		t.Fatal("signal should be kept")
	}
	if !o.keep(pubsub.ObservabilityEvent{Phase: "lifecycle", Status: "armed"}) {
		t.Fatal("lifecycle event should always be kept")
	}
}
