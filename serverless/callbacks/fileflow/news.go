package fileflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xd-dash/logma/serverless/callbacks/gdrive"
	"github.com/xd-dash/logma/serverless/pubsub"
)

type NewsFlow struct {
	BaseDir  string
	Drives   *gdrive.Registry
	FileName string

	mu sync.Mutex
}

type UploadTrigger struct {
	TriggerID string `json:"trigger_id,omitempty"`
	FolderID  string `json:"folder_id,omitempty"`
	Name      string `json:"name,omitempty"`
	MIMEType  string `json:"mime_type,omitempty"`
}

type NewsUploadResult struct {
	Scope      Scope               `json:"scope"`
	SourcePath string              `json:"source_path"`
	Snapshot   string              `json:"snapshot"`
	UploadID   string              `json:"upload_id"`
	Drive      gdrive.UploadResult `json:"drive"`
}

func NewNewsFlow(baseDir string, drives *gdrive.Registry) *NewsFlow {
	return &NewsFlow{BaseDir: baseDir, Drives: drives}
}

func (f *NewsFlow) Handlers(ctx context.Context, scope Scope) (pubsub.ChannelHandlers, error) {
	channels, err := NewsChannels(scope)
	if err != nil {
		return nil, err
	}
	if f == nil || f.Drives == nil {
		return nil, fmt.Errorf("news flow and Google Drive registry are required")
	}
	return pubsub.ChannelHandlers{
		channels.Write: func(payload string) {
			if _, err := f.Append(scope, payload); err != nil {
				log.Printf("callbacks/news: write failed scope=%+v: %v", scope, err)
			}
		},
		channels.Upload: func(payload string) {
			var trigger UploadTrigger
			if strings.TrimSpace(payload) != "" {
				if err := json.Unmarshal([]byte(payload), &trigger); err != nil {
					log.Printf("callbacks/news: decode upload trigger scope=%+v: %v", scope, err)
					return
				}
			}
			if _, err := f.Upload(ctx, scope, trigger); err != nil {
				log.Printf("callbacks/news: upload failed scope=%+v: %v", scope, err)
			}
		},
	}, nil
}

func (f *NewsFlow) Append(scope Scope, payload string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.appendLocked(scope, payload)
}

func (f *NewsFlow) appendLocked(scope Scope, payload string) (string, error) {
	path, err := f.path(scope)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", fmt.Errorf("create news callback directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return "", fmt.Errorf("open news callback file: %w", err)
	}
	defer file.Close()
	line := strings.TrimSpace(payload)
	if line == "" {
		return path, nil
	}
	if !json.Valid([]byte(line)) {
		encoded, err := json.Marshal(map[string]string{"data": line})
		if err != nil {
			return "", fmt.Errorf("marshal news callback payload: %w", err)
		}
		line = string(encoded)
	}
	if _, err := io.WriteString(file, line+"\n"); err != nil {
		return "", fmt.Errorf("append news callback file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return "", fmt.Errorf("sync news callback file: %w", err)
	}
	return path, nil
}

func (f *NewsFlow) Upload(ctx context.Context, scope Scope, trigger UploadTrigger) (NewsUploadResult, error) {
	uploader, err := f.Drives.Resolve(scope.CredentialID)
	if err != nil {
		return NewsUploadResult{}, err
	}

	f.mu.Lock()
	source, err := f.path(scope)
	if err != nil {
		f.mu.Unlock()
		return NewsUploadResult{}, err
	}
	if _, err := os.Stat(source); err != nil {
		f.mu.Unlock()
		return NewsUploadResult{}, fmt.Errorf("stat news callback file: %w", err)
	}
	dir := filepath.Dir(source)
	if err := os.MkdirAll(filepath.Join(dir, ".snapshots"), 0o750); err != nil {
		f.mu.Unlock()
		return NewsUploadResult{}, fmt.Errorf("create news snapshot directory: %w", err)
	}
	snapshot := filepath.Join(dir, ".snapshots", fmt.Sprintf("news-%d.ndjson", time.Now().UTC().UnixNano()))
	if err := copyFile(source, snapshot); err != nil {
		f.mu.Unlock()
		return NewsUploadResult{}, err
	}
	f.mu.Unlock()
	defer os.Remove(snapshot)

	hash, err := fileSHA256(snapshot)
	if err != nil {
		return NewsUploadResult{}, err
	}
	key, err := scope.Key()
	if err != nil {
		return NewsUploadResult{}, err
	}
	uploadID := deterministicID("news", key, hash)
	name := trigger.Name
	if name == "" {
		name = fmt.Sprintf("news-%s.ndjson", hash[:16])
	}
	mimeType := trigger.MIMEType
	if mimeType == "" {
		mimeType = "application/x-ndjson"
	}
	result, err := uploader.Upload(ctx, gdrive.UploadRequest{
		Path: snapshot, Name: name, FolderID: trigger.FolderID, MIMEType: mimeType, UploadID: uploadID,
		AppProperties: map[string]string{
			"logma_kind": "news", "logma_credential_id": scope.CredentialID,
			"logma_request_id": scope.RequestID, "logma_subscriber_id": scope.SubscriberID,
			"logma_content_sha256": hash,
		},
	})
	if err != nil {
		return NewsUploadResult{}, err
	}
	return NewsUploadResult{Scope: scope, SourcePath: source, Snapshot: snapshot, UploadID: uploadID, Drive: result}, nil
}

func (f *NewsFlow) path(scope Scope) (string, error) {
	dir, err := scope.Dir(f.BaseDir)
	if err != nil {
		return "", err
	}
	name := f.FileName
	if name == "" {
		name = "news.ndjson"
	}
	return filepath.Join(dir, name), nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open snapshot source: %w", err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	ok := false
	defer func() {
		_ = out.Close()
		if !ok {
			_ = os.Remove(dst)
		}
	}()
	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy snapshot: %w", err)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("sync snapshot: %w", err)
	}
	ok = true
	return nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file for hash: %w", err)
	}
	defer file.Close()
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", fmt.Errorf("hash file: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
