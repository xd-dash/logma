package pubsub

import "testing"

func TestParseShutdownRequestSupportsLifecycleSignal(t *testing.T) {
	direct := ParseShutdownRequest(`{"reason":"operator"}`)
	if direct.Reason != "operator" {
		t.Fatalf("direct reason = %q", direct.Reason)
	}

	signal := ParseShutdownRequest(`{
		"type":"Signal",
		"message":{
			"signal":"shutdown",
			"condition":"timer"
		}
	}`)
	if signal.Reason != "lifecycle:timer" {
		t.Fatalf("signal reason = %q", signal.Reason)
	}
}
