package pubsubruntime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var webhookHTTPClient = &http.Client{Timeout: 30 * time.Second}

func postWebhook(ctx context.Context, callbackURL string, payload string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, strings.NewReader(payload))
	if err != nil {
		// Callback URLs are validated before runtime materialization. Do not copy
		// the raw URL back into an error if an unexpected request-construction
		// failure still occurs.
		return errors.New("build webhook request")
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := webhookHTTPClient.Do(req)
	if err != nil {
		// net/http normally wraps transport failures in *url.Error, whose Error()
		// string contains the request URL. Callback URLs may carry signed/query
		// material, so preserve the underlying failure/cancellation while
		// stripping the endpoint from the error that reaches logs.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			return fmt.Errorf("webhook request failed: %w", urlErr.Err)
		}
		return errors.New("webhook request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}
