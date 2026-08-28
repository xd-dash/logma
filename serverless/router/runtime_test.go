package router

import (
	"encoding/json"
	"testing"
)

func TestHandlePublishWrapsRawJSON(t *testing.T) {
	rt := NewRuntime()
	payload := `{"id":"news-1","title":"headline"}`

	rt.handlePublish(runtimeMessage{channel: "news:item:all:global", payload: payload})

	select {
	case got := <-rt.events:
		if got.Channel != "news:item:all:global" {
			t.Fatalf("channel = %q", got.Channel)
		}
		var data map[string]any
		if err := json.Unmarshal(got.Data, &data); err != nil {
			t.Fatalf("unmarshal data: %v", err)
		}
		if data["id"] != "news-1" || data["title"] != "headline" {
			t.Fatalf("unexpected data: %#v", data)
		}
	default:
		t.Fatal("expected publish event")
	}
}

func TestHandlePublishPreservesEnvelope(t *testing.T) {
	rt := NewRuntime()
	payload := `{"channel":"custom","data":{"id":"news-2"}}`

	rt.handlePublish(runtimeMessage{channel: "redis-channel", payload: payload})

	select {
	case got := <-rt.events:
		if got.Channel != "custom" {
			t.Fatalf("channel = %q", got.Channel)
		}
		if string(got.Data) != `{"id":"news-2"}` {
			t.Fatalf("data = %s", got.Data)
		}
	default:
		t.Fatal("expected publish event")
	}
}
