// Package axiom provides an optional, best-effort Axiom observer for Logma
// serverless/Fatline runtimes. It is disabled unless explicitly enabled and
// never participates in publication, lifecycle, or teardown authority.
package axiom

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xd-dash/logma/serverless/pubsub"
)

const DefaultDataset = "dev.global"

type PublishMode string

const (
	PublishNone      PublishMode = "none"
	PublishImportant PublishMode = "important"
	PublishAll       PublishMode = "all"
)

type Config struct {
	Enabled     bool
	Token       string
	Dataset     string
	Domain      string
	PublishMode PublishMode
	QueueSize   int
	Timeout     time.Duration

	// Static fields identify the live Fatline independently from any one
	// invocation. Typical keys are environment, mode, deployment_id,
	// fatline_id, composition_id, source_sha, and region.
	Static map[string]any

	// ImportantPublish may override the built-in bounded-channel classifier.
	// It is used only when PublishMode == PublishImportant.
	ImportantPublish func(channel string, payload json.RawMessage) bool
}

type Observer struct {
	cfg     Config
	client  *http.Client
	queue   chan pubsub.ObservabilityEvent
	done    chan struct{}
	close   sync.Once
	dropped atomic.Uint64
	sent    atomic.Uint64
	failed  atomic.Uint64
}

func New(cfg Config) *Observer {
	if cfg.Dataset == "" {
		cfg.Dataset = DefaultDataset
	}
	if cfg.Domain == "" {
		cfg.Domain = "api.axiom.co"
	}
	if cfg.PublishMode == "" {
		cfg.PublishMode = PublishImportant
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	o := &Observer{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.Timeout},
		queue:  make(chan pubsub.ObservabilityEvent, cfg.QueueSize),
		done:   make(chan struct{}),
	}
	if cfg.Enabled && cfg.Token != "" {
		go o.run()
	} else {
		close(o.done)
	}
	return o
}

func (o *Observer) Observe(_ context.Context, event pubsub.ObservabilityEvent) {
	if o == nil || !o.cfg.Enabled || o.cfg.Token == "" || !o.keep(event) {
		return
	}
	select {
	case o.queue <- event:
	default:
		o.dropped.Add(1)
	}
}

func (o *Observer) Dropped() uint64 {
	if o == nil {
		return 0
	}
	return o.dropped.Load()
}

// Sent is the number of events for which Axiom returned a 2xx response.
func (o *Observer) Sent() uint64 {
	if o == nil {
		return 0
	}
	return o.sent.Load()
}

// Failed is the number of queued events whose ingest request failed or returned
// a non-2xx response. It is observation evidence only and never data-plane authority.
func (o *Observer) Failed() uint64 {
	if o == nil {
		return 0
	}
	return o.failed.Load()
}

func (o *Observer) Close(ctx context.Context) error {
	if o == nil || !o.cfg.Enabled || o.cfg.Token == "" {
		return nil
	}
	o.close.Do(func() { close(o.queue) })
	select {
	case <-o.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *Observer) keep(event pubsub.ObservabilityEvent) bool {
	if event.Phase != "publish" {
		return true
	}
	switch o.cfg.PublishMode {
	case PublishAll:
		return true
	case PublishNone:
		return false
	case PublishImportant:
		if o.cfg.ImportantPublish != nil {
			return o.cfg.ImportantPublish(event.Channel, event.Payload)
		}
		return importantChannel(event.Channel)
	default:
		return false
	}
}

func importantChannel(channel string) bool {
	channel = strings.ToLower(channel)
	for _, marker := range []string{
		"signal", "alert", "news", "decision", "opportunity", "execution",
		"lifecycle", "shutdown", "policy", "anomaly", "error",
	} {
		if strings.Contains(channel, marker) {
			return true
		}
	}
	return false
}

func (o *Observer) run() {
	defer close(o.done)
	for event := range o.queue {
		if err := o.send(event); err != nil {
			o.failed.Add(1)
		} else {
			o.sent.Add(1)
		}
	}
}

func (o *Observer) send(event pubsub.ObservabilityEvent) error {
	body := make(map[string]any, len(o.cfg.Static)+16)
	for key, value := range o.cfg.Static {
		body[key] = value
	}
	body["_time"] = event.Time.UTC().Format(time.RFC3339Nano)
	body["kind"] = event.Kind
	body["phase"] = event.Phase
	put(body, "status", event.Status)
	put(body, "namespace", event.Namespace)
	put(body, "instance_id", event.InstanceID)
	put(body, "request_id", event.RequestID)
	put(body, "channel", event.Channel)
	put(body, "policy", event.Policy)
	if event.PolicyCode != 0 {
		body["policy_code"] = event.PolicyCode
	}
	put(body, "reason", event.Reason)
	if len(event.Payload) > 0 {
		var payload any
		if json.Unmarshal(event.Payload, &payload) == nil {
			body["payload"] = payload
		}
	}
	if dropped := o.dropped.Swap(0); dropped > 0 {
		body["observer_dropped_since_last_send"] = dropped
	}

	encoded, err := json.Marshal([]map[string]any{body})
	if err != nil {
		return err
	}
	endpoint, err := ingestURL(o.cfg.Domain, o.cfg.Dataset)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+o.cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "logma-fatline-axiom/1")
	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &httpStatusError{status: resp.StatusCode}
	}
	return nil
}

func ingestURL(domain, dataset string) (string, error) {
	domain = strings.TrimSpace(strings.ToLower(domain))
	domain = strings.TrimPrefix(domain, "https://")
	domain = strings.TrimPrefix(domain, "http://")
	domain = strings.TrimSuffix(domain, "/")
	if domain == "" || strings.ContainsAny(domain, "/@") {
		return "", &url.Error{Op: "parse", URL: domain, Err: errInvalidDomain{}}
	}
	path := url.PathEscape(dataset)
	if domain == "api.axiom.co" {
		return "https://" + domain + "/v1/datasets/" + path + "/ingest", nil
	}
	return "https://" + domain + "/v1/ingest/" + path, nil
}

func put(dst map[string]any, key, value string) {
	if value != "" {
		dst[key] = value
	}
}

type httpStatusError struct{ status int }

func (e *httpStatusError) Error() string { return http.StatusText(e.status) }

type errInvalidDomain struct{}

func (errInvalidDomain) Error() string { return "invalid domain" }
