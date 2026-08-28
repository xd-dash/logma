package pubsub

import (
	"context"
	"testing"
)

type testLifecycle struct {
	Session
}

func newTestLifecycle() *testLifecycle {
	return &testLifecycle{Session: NewSession()}
}

func (l *testLifecycle) Start(ctx context.Context) {
	l.Begin(ctx, func() { <-l.Context().Done() })
}

func TestHolderMintsFreshRuntimeAfterCompletion(t *testing.T) {
	holder := NewHolder(newTestLifecycle)

	first, ok := holder.Claim()
	if !ok {
		t.Fatal("first runtime was not claimable")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		first.Start(ctx)
		close(done)
	}()
	cancel()
	<-done

	second, ok := holder.Claim()
	if !ok {
		t.Fatal("holder did not mint a fresh runtime after completion")
	}
	if first == second {
		t.Fatal("holder reused completed runtime")
	}
	second.Cancel()
}
