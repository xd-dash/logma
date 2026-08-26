package events

import "testing"

func TestNewerThan(t *testing.T) {
	e := Event{Version: 17}
	if !e.NewerThan(16) {
		t.Fatal("expected newer event")
	}
	if e.NewerThan(17) || e.NewerThan(18) {
		t.Fatal("same or older event must not be newer")
	}
}
