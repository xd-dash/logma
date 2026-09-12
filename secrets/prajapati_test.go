package secrets

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArtifactDigestAndPrajapatiDecrypt(t *testing.T) {
	ciphertext := []byte("wrapped-secret")
	bindingDigest := "sha256:" + strings.Repeat("a", 64)
	artifact := NewArtifact("axiom-token", 8, "axiom-token", ciphertext, bindingDigest)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/keys/axiom-token/decrypt" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer AT1.test" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var request decryptRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		raw, err := base64.StdEncoding.DecodeString(request.Data)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != string(ciphertext) {
			t.Fatalf("ciphertext=%q", raw)
		}
		_ = json.NewEncoder(w).Encode(decryptResponse{Data: base64.StdEncoding.EncodeToString([]byte("axiom-plaintext"))})
	}))
	defer server.Close()

	client := Client{BaseURL: server.URL, BindingDigest: bindingDigest, HTTPClient: server.Client()}
	plaintext, err := client.Decrypt(context.Background(), artifact, "AT1.test")
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != "axiom-plaintext" {
		t.Fatalf("plaintext=%q", plaintext)
	}

	client.BindingDigest = "sha256:" + strings.Repeat("d", 64)
	if _, err := client.Decrypt(context.Background(), artifact, "AT1.test"); err == nil {
		t.Fatal("expected mismatched Fatline binding digest to be rejected")
	}
}

func TestArtifactRejectsTamperedCiphertext(t *testing.T) {
	artifact := NewArtifact("axiom-token", 1, "axiom-token", []byte("ciphertext"), "sha256:"+strings.Repeat("b", 64))
	artifact.Ciphertext = base64.StdEncoding.EncodeToString([]byte("tampered"))
	if _, err := artifact.CiphertextBytes(); err == nil {
		t.Fatal("expected ciphertext digest mismatch")
	}
}

func TestMaterializeUsesPrivateAtomicFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime", "axiom-token")
	if err := Materialize(path, []byte("secret")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode=%#o", info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "secret" {
		t.Fatalf("data=%q", data)
	}
}

func TestNewArtifactDigest(t *testing.T) {
	raw := []byte("ciphertext")
	artifact := NewArtifact("s", 1, "k", raw, "sha256:"+strings.Repeat("c", 64))
	sum := sha256.Sum256(raw)
	want := "sha256:" + hex.EncodeToString(sum[:])
	if artifact.CiphertextSHA != want {
		t.Fatalf("digest=%q want=%q", artifact.CiphertextSHA, want)
	}
}
