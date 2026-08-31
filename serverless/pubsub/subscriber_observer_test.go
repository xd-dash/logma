package pubsub

import (
	"context"
	"encoding/json"
	"testing"
)

type captureObserver struct{ events []ObservabilityEvent }

func (o *captureObserver) Observe(_ context.Context, event ObservabilityEvent) {
	o.events = append(o.events, event)
}

func TestObserveSubscriberMessagePreservesJSONPayload(t *testing.T) {
	o := &captureObserver{}
	ObserveSubscriberMessage(o, context.Background(), "scope:stonks:quote:AAPL", `{"symbol":"AAPL","bid_price":1}`)
	if len(o.events) != 1 {
		t.Fatalf("events=%d", len(o.events))
	}
	e := o.events[0]
	if e.Kind != "publication" || e.Phase != "subscriber" || e.Status != "received" {
		t.Fatalf("unexpected event: %+v", e)
	}
	if e.Channel != "scope:stonks:quote:AAPL" {
		t.Fatalf("channel=%q", e.Channel)
	}
	var body map[string]any
	if err := json.Unmarshal(e.Payload, &body); err != nil {
		t.Fatal(err)
	}
	if body["symbol"] != "AAPL" {
		t.Fatalf("payload=%v", body)
	}
}

func TestObserveSubscriberMessageWrapsNonJSONPayload(t *testing.T) {
	o := &captureObserver{}
	ObserveSubscriberMessage(o, context.Background(), "scope:test", "plain")
	if len(o.events) != 1 {
		t.Fatalf("events=%d", len(o.events))
	}
	var value string
	if err := json.Unmarshal(o.events[0].Payload, &value); err != nil {
		t.Fatal(err)
	}
	if value != "plain" {
		t.Fatalf("value=%q", value)
	}
}
