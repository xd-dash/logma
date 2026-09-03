package pubsub

import "testing"

func TestSubscriberReadyTracksCurrentConnectionGeneration(t *testing.T) {
	s := &Subscriber{stopped: make(chan struct{}), ready: make(chan struct{})}

	initial := s.Ready()
	select {
	case <-initial:
		t.Fatal("new Subscriber unexpectedly starts ready")
	default:
	}

	s.markReady()
	select {
	case <-initial:
	default:
		t.Fatal("initial readiness signal did not close")
	}
	if got := s.Ready(); got != initial {
		t.Fatal("ready Subscriber replaced its readiness channel without disconnecting")
	}

	s.markNotReady()
	reconnecting := s.Ready()
	if reconnecting == initial {
		t.Fatal("disconnect did not create a fresh readiness generation")
	}
	select {
	case <-reconnecting:
		t.Fatal("reconnecting Subscriber still reports historical readiness")
	default:
	}

	s.markReady()
	select {
	case <-reconnecting:
	default:
		t.Fatal("reconnected readiness signal did not close")
	}
}
