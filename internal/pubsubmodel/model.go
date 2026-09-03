package pubsubmodel

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// Channel is a Logma-owned active Redis listening resource. A channel may
// exist without any attached Subscriber resources.
type Channel struct {
	Name string `json:"name"`
}

func (c Channel) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return errors.New("channel name is required")
	}
	return nil
}

// CallbackType identifies the execution mechanism for a Callback resource.
type CallbackType string

const (
	CallbackWebhook CallbackType = "webhook"
	CallbackLua     CallbackType = "lua"
)

// Callback is an independently addressable action that can be attached to one
// or more Subscriber resources.
type Callback struct {
	ID      string           `json:"id"`
	Type    CallbackType     `json:"type"`
	Webhook *WebhookCallback `json:"webhook,omitempty"`
	Lua     *LuaCallback     `json:"lua,omitempty"`
}

// WebhookCallback preserves the historical Logma ability for one callback
// definition to fan out to one or many HTTP endpoints. CallbackURL is the
// compatibility/single-target form; CallbackURLs is the multi-target form.
// At least one non-empty HTTP(S) target is required.
type WebhookCallback struct {
	CallbackURL  string   `json:"callbackURL,omitempty"`
	CallbackURLs []string `json:"callbackURLs,omitempty"`
}

func (w WebhookCallback) URLs() []string {
	urls := make([]string, 0, 1+len(w.CallbackURLs))
	if target := strings.TrimSpace(w.CallbackURL); target != "" {
		urls = append(urls, target)
	}
	for _, target := range w.CallbackURLs {
		if target = strings.TrimSpace(target); target != "" {
			urls = append(urls, target)
		}
	}
	return urls
}

func validateWebhookURL(value string) error {
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("webhook callback URL %q must be an absolute http or https URL", value)
	}
	return nil
}

type LuaCallback struct {
	Name string `json:"name"`
}

func (c Callback) Validate() error {
	if strings.TrimSpace(c.ID) == "" {
		return errors.New("callback id is required")
	}

	switch c.Type {
	case CallbackWebhook:
		if c.Webhook == nil || len(c.Webhook.URLs()) == 0 {
			return errors.New("webhook requires at least one callback URL")
		}
		if c.Lua != nil {
			return errors.New("webhook callback cannot include lua configuration")
		}
		for _, target := range c.Webhook.URLs() {
			if err := validateWebhookURL(target); err != nil {
				return err
			}
		}
	case CallbackLua:
		if c.Lua == nil || strings.TrimSpace(c.Lua.Name) == "" {
			return errors.New("lua callback name is required")
		}
		if c.Webhook != nil {
			return errors.New("lua callback cannot include webhook configuration")
		}
	default:
		return fmt.Errorf("unsupported callback type %q", c.Type)
	}

	return nil
}

// Subscriber is a durable attachment between an active Channel and one or
// more Callback resources. Unlike Channel, a Subscriber is invalid without a
// callback.
type Subscriber struct {
	ID          string   `json:"id"`
	Channel     string   `json:"channel"`
	CallbackIDs []string `json:"callbackIDs"`
}

func (s Subscriber) Validate() error {
	if strings.TrimSpace(s.ID) == "" {
		return errors.New("subscriber id is required")
	}
	if strings.TrimSpace(s.Channel) == "" {
		return errors.New("subscriber channel is required")
	}
	if len(s.CallbackIDs) == 0 {
		return errors.New("subscriber requires at least one callback")
	}
	for _, id := range s.CallbackIDs {
		if strings.TrimSpace(id) == "" {
			return errors.New("subscriber callback id is empty")
		}
	}
	return nil
}

// Publisher represents a producer binding. Creating/activating a Publisher is
// expected to ensure its Channel exists before the producer starts. Config is
// deliberately opaque here so producer-specific contracts remain owned by the
// producer integration rather than the generic Pub/Sub model.
type Publisher struct {
	ID      string          `json:"id"`
	Channel string          `json:"channel"`
	Type    string          `json:"type"`
	Config  json.RawMessage `json:"config,omitempty"`
}

func (p Publisher) Validate() error {
	if strings.TrimSpace(p.ID) == "" {
		return errors.New("publisher id is required")
	}
	if strings.TrimSpace(p.Channel) == "" {
		return errors.New("publisher channel is required")
	}
	if strings.TrimSpace(p.Type) == "" {
		return errors.New("publisher type is required")
	}
	return nil
}

// SubscriptionGroup is durable metadata plus an unordered set of Subscriber
// identities. Group membership is persisted separately from this hash-shaped
// metadata so Redis can answer membership questions without decoding JSON.
type SubscriptionGroup struct {
	ID            string   `json:"id"`
	SubscriberIDs []string `json:"subscriberIDs,omitempty"`
}

func (g SubscriptionGroup) Validate() error {
	if strings.TrimSpace(g.ID) == "" {
		return errors.New("subscription group id is required")
	}
	for _, id := range g.SubscriberIDs {
		if strings.TrimSpace(id) == "" {
			return errors.New("subscription group subscriber id is empty")
		}
	}
	return nil
}

// ServerlessEndpoint describes requester-driven delivery capability such as
// SSE. It is intentionally not a Subscriber: the endpoint can exist without a
// standing Redis subscription and creates request-scoped event delivery when
// invoked.
type ServerlessEndpoint struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Path string `json:"path"`
}

func (e ServerlessEndpoint) Validate() error {
	if strings.TrimSpace(e.ID) == "" {
		return errors.New("serverless endpoint id is required")
	}
	if strings.TrimSpace(e.Type) == "" {
		return errors.New("serverless endpoint type is required")
	}
	if strings.TrimSpace(e.Path) == "" {
		return errors.New("serverless endpoint path is required")
	}
	return nil
}
