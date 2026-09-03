package pubsubruntime

import (
	"context"
	"testing"

	"github.com/xd-dash/logma/internal/pubsubmodel"
)

type contextCapturingPublisherProvider struct {
	ctx context.Context
}

func (p *contextCapturingPublisherProvider) EnsureActive(ctx context.Context, _ pubsubmodel.Publisher, _ pubsubmodel.Channel) error {
	p.ctx = ctx
	return nil
}

func TestPublisherReconcilerRuntimeSurvivesRequestCancellation(t *testing.T) {
	store := publisherTestStore{
		publisher: pubsubmodel.Publisher{ID: "stonks", Channel: "market", Type: "capture"},
		channel:   pubsubmodel.Channel{Name: "market"},
	}
	providers := NewPublisherRegistry()
	provider := &contextCapturingPublisherProvider{}
	if err := providers.Register("capture", provider); err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewPublisherReconciler(store, providers)
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	if err := reconciler.Reconcile(requestCtx, "stonks"); err != nil {
		t.Fatal(err)
	}
	cancelRequest()
	select {
	case <-provider.ctx.Done():
		t.Fatal("successful Publisher runtime inherited request cancellation")
	default:
	}
	reconciler.Close()
	select {
	case <-provider.ctx.Done():
	default:
		t.Fatal("Publisher runtime context was not canceled by reconciler Close")
	}
}
