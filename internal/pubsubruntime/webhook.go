package pubsubruntime

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

var webhookHTTPClient = &http.Client{Timeout: 30 * time.Second}

func postWebhook(ctx context.Context, url string, payload string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := webhookHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}
