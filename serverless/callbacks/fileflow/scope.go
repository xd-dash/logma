package fileflow

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

var safePart = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

type Scope struct {
	CredentialID string `json:"credential_id"`
	RequestID    string `json:"request_id"`
	SubscriberID string `json:"subscriber_id"`
}

type Channels struct {
	Write  string `json:"write"`
	Upload string `json:"upload"`
}

func (s Scope) Validate() error {
	parts := map[string]string{
		"credential_id": s.CredentialID,
		"request_id":    s.RequestID,
		"subscriber_id": s.SubscriberID,
	}
	for name, value := range parts {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
		if !safePart.MatchString(value) {
			return fmt.Errorf("%s contains unsafe characters", name)
		}
	}
	return nil
}

func (s Scope) Key() (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	return strings.Join([]string{s.CredentialID, s.RequestID, s.SubscriberID}, ":"), nil
}

func (s Scope) Dir(base string) (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	if strings.TrimSpace(base) == "" {
		return "", errors.New("base directory is required")
	}
	return filepath.Join(base, s.CredentialID, s.RequestID, s.SubscriberID), nil
}

func NewsChannels(s Scope) (Channels, error) {
	return channelsFor(s, "news")
}

func MarketChannels(s Scope) (Channels, error) {
	return channelsFor(s, "market")
}

func channelsFor(s Scope, kind string) (Channels, error) {
	key, err := s.Key()
	if err != nil {
		return Channels{}, err
	}
	prefix := "scope:gdrive:" + strings.ReplaceAll(key, ":", ":request:")
	// Keep the channel readable while the filesystem key remains compact.
	prefix = "scope:gdrive:" + s.CredentialID + ":request:" + s.RequestID + ":subscriber:" + s.SubscriberID + ":" + kind
	return Channels{Write: prefix + ":write", Upload: prefix + ":upload"}, nil
}

func deterministicID(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
