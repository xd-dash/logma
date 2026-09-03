package pubsubmodel

import (
	"errors"
	"strings"
)

// PublisherGroup is durable metadata plus an unordered set of Publisher
// identities. Membership is stored as graph edges rather than embedded JSON.
type PublisherGroup struct {
	ID           string   `json:"id"`
	PublisherIDs []string `json:"publisherIDs,omitempty"`
}

func (g PublisherGroup) Validate() error {
	if strings.TrimSpace(g.ID) == "" {
		return errors.New("publisher group id is required")
	}
	for _, id := range g.PublisherIDs {
		if strings.TrimSpace(id) == "" {
			return errors.New("publisher group publisher id is empty")
		}
	}
	return nil
}
