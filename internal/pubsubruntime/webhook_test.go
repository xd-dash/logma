package pubsubruntime

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestPostWebhookSanitizesTransportURL(t *testing.T) {
	original := webhookHTTPClient
	webhookHTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed")
	})}
	t.Cleanup(func() { webhookHTTPClient = original })

	const secret = "signed-secret-value"
	err := postWebhook(context.Background(), "https://example.invalid/hook?signature="+secret, `{}`)
	if err == nil {
		t.Fatal("postWebhook unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "example.invalid") {
		t.Fatalf("webhook error leaked callback URL material: %q", err)
	}
	if !strings.Contains(err.Error(), "dial failed") {
		t.Fatalf("webhook error lost underlying transport diagnostic: %q", err)
	}
}
