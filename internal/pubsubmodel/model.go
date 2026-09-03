package pubsubmodel

import (
	"encoding/json"
	"errors"
	"fmt"
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

type WebhookCallback struct {
	CallbackURL string `json:"callbackURL"`
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
		if c.Webhook == nil || strings.TrimSpace(c.Webhook.CallbackURL) == "" {
			return errors.New("webhook callbackURL is required")
		}
		if c.Lua != nil {
			return errors.New("webhook callback cannot include lua configuration")
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
