// Package secrets resolves encrypted runtime artifacts through Prajapati.
//
// Logma may persist or distribute ciphertext metadata, but it never receives a
// Marai master key or Marai Redis credential. The caller supplies a short-lived
// Prajapati bearer credential and the approved plaintext is materialized only at
// the runtime boundary that needs it.
package secrets

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// Artifact is durable ciphertext metadata. Ciphertext is standard-base64 so the
// representation can safely cross JSON/Redis/object-store boundaries.
type Artifact struct {
	Version       uint8  `json:"version"`
	SecretID      string `json:"secret_id"`
	Revision      uint64 `json:"revision"`
	KeyID         string `json:"key_id"`
	Ciphertext    string `json:"ciphertext"`
	CiphertextSHA string `json:"ciphertext_sha256"`
	BindingDigest string `json:"binding_digest"`
}

func (a Artifact) CiphertextBytes() ([]byte, error) {
	if a.Version != 1 || a.SecretID == "" || a.Revision == 0 || a.KeyID == "" || a.BindingDigest == "" {
		return nil, errors.New("incomplete secret artifact")
	}
	raw, err := base64.StdEncoding.DecodeString(a.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext: %w", err)
	}
	sum := sha256.Sum256(raw)
	want := "sha256:" + hex.EncodeToString(sum[:])
	if a.CiphertextSHA != want {
		return nil, fmt.Errorf("ciphertext digest mismatch: got %q want %q", a.CiphertextSHA, want)
	}
	return raw, nil
}

func NewArtifact(secretID string, revision uint64, keyID string, ciphertext []byte, bindingDigest string) Artifact {
	sum := sha256.Sum256(ciphertext)
	return Artifact{
		Version:       1,
		SecretID:      secretID,
		Revision:      revision,
		KeyID:         keyID,
		Ciphertext:    base64.StdEncoding.EncodeToString(ciphertext),
		CiphertextSHA: "sha256:" + hex.EncodeToString(sum[:]),
		BindingDigest: bindingDigest,
	}
}

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

type decryptRequest struct {
	Data string `json:"data"`
}

type decryptResponse struct {
	Data string `json:"data"`
}

// Decrypt asks Prajapati to authorize and broker one Marai decrypt operation.
// credential is a Prajapati identity credential (for example a short-lived AT1),
// not a Redis password.
func (c Client) Decrypt(ctx context.Context, artifact Artifact, credential string) ([]byte, error) {
	ciphertext, err := artifact.CiphertextBytes()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(c.BaseURL) == "" || strings.TrimSpace(credential) == "" {
		return nil, errors.New("Prajapati base URL and credential are required")
	}
	payload, err := json.Marshal(decryptRequest{Data: base64.StdEncoding.EncodeToString(ciphertext)})
	if err != nil {
		return nil, err
	}
	url := strings.TrimRight(c.BaseURL, "/") + "/v1/keys/" + artifact.KeyID + "/decrypt"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+credential)
	req.Header.Set("Content-Type", "application/json")
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Prajapati decrypt request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Prajapati decrypt denied or failed: status=%d", resp.StatusCode)
	}
	var decoded decryptResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("decode Prajapati response: %w", err)
	}
	plaintext, err := base64.StdEncoding.DecodeString(decoded.Data)
	if err != nil {
		return nil, fmt.Errorf("decode Prajapati plaintext: %w", err)
	}
	return plaintext, nil
}

// Materialize writes the resolved plaintext atomically with mode 0600. The
// directory is caller-owned runtime state (normally /run), never a durable
// secret store.
func Materialize(path string, plaintext []byte) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("secret materialization path is required")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".secret-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(plaintext); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
