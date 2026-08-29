package fileflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xd-dash/logma/serverless/callbacks/gdrive"
	"github.com/xd-dash/logma/serverless/pubsub"
)

var ErrNoClosedMarketSegment = errors.New("no closed market segment is available")

type MarketFlow struct {
	BaseDir string
	Drives  *gdrive.Registry

	mu      sync.Mutex
	writers map[string]*activeWriter
}

type activeWriter struct {
	path string
	file *os.File
	seq  uint64
}

type pendingMarketUpload struct {
	Scope     Scope  `json:"scope"`
	TriggerID string `json:"trigger_id"`
	Path      string `json:"path"`
	UploadID  string `json:"upload_id"`
}

type MarketUploadResult struct {
	Scope      Scope               `json:"scope"`
	TriggerID  string              `json:"trigger_id"`
	ClosedPath string              `json:"closed_path"`
	NewPath    string              `json:"new_path"`
	UploadID   string              `json:"upload_id"`
	Drive      gdrive.UploadResult `json:"drive"`
}

func NewMarketFlow(baseDir string, drives *gdrive.Registry) *MarketFlow {
	return &MarketFlow{BaseDir: baseDir, Drives: drives, writers: make(map[string]*activeWriter)}
}

func (f *MarketFlow) Handlers(ctx context.Context, scope Scope) (pubsub.ChannelHandlers, error) {
	channels, err := MarketChannels(scope)
	if err != nil {
		return nil, err
	}
	if f == nil || f.Drives == nil {
		return nil, fmt.Errorf("market flow and Google Drive registry are required")
	}
	return pubsub.ChannelHandlers{
		channels.Write: func(payload string) {
			if _, err := f.Write(scope, payload); err != nil {
				log.Printf("callbacks/market: write failed scope=%+v: %v", scope, err)
			}
		},
		channels.Upload: func(payload string) {
			var trigger UploadTrigger
			if err := json.Unmarshal([]byte(payload), &trigger); err != nil {
				log.Printf("callbacks/market: decode upload trigger scope=%+v: %v", scope, err)
				return
			}
			if strings.TrimSpace(trigger.TriggerID) == "" {
				log.Printf("callbacks/market: trigger_id is required scope=%+v", scope)
				return
			}
			if _, err := f.RotateUploadDelete(ctx, scope, trigger); err != nil {
				log.Printf("callbacks/market: rotate/upload failed scope=%+v trigger=%q: %v", scope, trigger.TriggerID, err)
			}
		},
	}, nil
}

func (f *MarketFlow) Write(scope Scope, payload string) (string, error) {
	key, err := scope.Key()
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	writer, err := f.ensureWriterLocked(scope, key)
	if err != nil {
		return "", err
	}
	line := strings.TrimSpace(payload)
	if line == "" {
		return writer.path, nil
	}
	if !json.Valid([]byte(line)) {
		encoded, err := json.Marshal(map[string]string{"data": line})
		if err != nil {
			return "", fmt.Errorf("marshal market callback payload: %w", err)
		}
		line = string(encoded)
	}
	if _, err := writer.file.WriteString(line + "\n"); err != nil {
		return "", fmt.Errorf("append market stream: %w", err)
	}
	return writer.path, nil
}

func (f *MarketFlow) ActivePath(scope Scope) (string, bool) {
	key, err := scope.Key()
	if err != nil {
		return "", false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	writer := f.writers[key]
	if writer == nil {
		return "", false
	}
	return writer.path, true
}

// RotateUploadDelete is idempotent per scope + trigger_id. The same trigger can
// safely be delivered repeatedly. A pending marker persists the closed segment
// before Drive I/O, so a failed upload resumes that segment rather than rotating
// the current writer again. Once Drive confirms the upload, the closed file and
// pending marker are deleted.
func (f *MarketFlow) RotateUploadDelete(ctx context.Context, scope Scope, trigger UploadTrigger) (MarketUploadResult, error) {
	if strings.TrimSpace(trigger.TriggerID) == "" {
		return MarketUploadResult{}, errors.New("trigger_id is required for market upload")
	}
	if f == nil || f.Drives == nil {
		return MarketUploadResult{}, errors.New("market flow and Google Drive registry are required")
	}
	uploader, err := f.Drives.Resolve(scope.CredentialID)
	if err != nil {
		return MarketUploadResult{}, err
	}
	key, err := scope.Key()
	if err != nil {
		return MarketUploadResult{}, err
	}
	uploadID := deterministicID("market", key, trigger.TriggerID)
	pendingPath, err := f.pendingPath(scope, trigger.TriggerID)
	if err != nil {
		return MarketUploadResult{}, err
	}
	pending, pendingErr := readPending(pendingPath)
	if pendingErr != nil && !errors.Is(pendingErr, os.ErrNotExist) {
		return MarketUploadResult{}, pendingErr
	}

	if existing, ok, err := uploader.FindByUploadID(ctx, uploadID); err != nil {
		return MarketUploadResult{}, err
	} else if ok {
		existing.AlreadyExists = true
		closed := ""
		if pending != nil {
			closed = pending.Path
			if err := cleanupPendingMarketUpload(pendingPath, pending.Path); err != nil {
				return MarketUploadResult{}, err
			}
		}
		return MarketUploadResult{Scope: scope, TriggerID: trigger.TriggerID, ClosedPath: closed, UploadID: uploadID, Drive: existing}, nil
	}

	newPath := ""
	if pending == nil {
		closedPath, nextPath, err := f.rotate(scope, key)
		if err != nil {
			return MarketUploadResult{}, err
		}
		newPath = nextPath
		if closedPath == "" {
			return MarketUploadResult{}, ErrNoClosedMarketSegment
		}
		pending = &pendingMarketUpload{Scope: scope, TriggerID: trigger.TriggerID, Path: closedPath, UploadID: uploadID}
		if err := writePending(pendingPath, *pending); err != nil {
			return MarketUploadResult{}, err
		}
	}

	name := trigger.Name
	if name == "" {
		name = filepath.Base(pending.Path)
	}
	mimeType := trigger.MIMEType
	if mimeType == "" {
		mimeType = "application/x-ndjson"
	}
	result, err := uploader.Upload(ctx, gdrive.UploadRequest{
		Path: pending.Path, Name: name, FolderID: trigger.FolderID,
		MIMEType: mimeType, UploadID: pending.UploadID,
		AppProperties: map[string]string{
			"logma_kind": "market", "logma_credential_id": scope.CredentialID,
			"logma_request_id": scope.RequestID, "logma_subscriber_id": scope.SubscriberID,
			"logma_trigger_id": trigger.TriggerID,
		},
	})
	if err != nil {
		return MarketUploadResult{Scope: scope, TriggerID: trigger.TriggerID, ClosedPath: pending.Path, NewPath: newPath, UploadID: pending.UploadID}, err
	}
	if err := cleanupPendingMarketUpload(pendingPath, pending.Path); err != nil {
		return MarketUploadResult{}, err
	}
	return MarketUploadResult{Scope: scope, TriggerID: trigger.TriggerID, ClosedPath: pending.Path, NewPath: newPath, UploadID: pending.UploadID, Drive: result}, nil
}

func (f *MarketFlow) Close() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var first error
	for key, writer := range f.writers {
		if writer != nil && writer.file != nil {
			if err := writer.file.Close(); err != nil && first == nil {
				first = err
			}
		}
		delete(f.writers, key)
	}
	return first
}

func (f *MarketFlow) rotate(scope Scope, key string) (closedPath, newPath string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	current, err := f.ensureWriterLocked(scope, key)
	if err != nil {
		return "", "", err
	}
	closedPath = current.path
	next, err := f.newWriterLocked(scope, current.seq+1)
	if err != nil {
		return "", "", err
	}
	// Swap first. Any later callback write sees only the new file.
	f.writers[key] = next
	if err := current.file.Sync(); err != nil {
		_ = next.file.Close()
		_ = os.Remove(next.path)
		f.writers[key] = current
		return "", "", fmt.Errorf("sync closed market segment: %w", err)
	}
	if err := current.file.Close(); err != nil {
		return "", next.path, fmt.Errorf("close market segment: %w", err)
	}
	return closedPath, next.path, nil
}

func (f *MarketFlow) ensureWriterLocked(scope Scope, key string) (*activeWriter, error) {
	if f.writers == nil {
		f.writers = make(map[string]*activeWriter)
	}
	if writer := f.writers[key]; writer != nil {
		return writer, nil
	}
	writer, err := f.newWriterLocked(scope, 1)
	if err != nil {
		return nil, err
	}
	f.writers[key] = writer
	return writer, nil
}

func (f *MarketFlow) newWriterLocked(scope Scope, seq uint64) (*activeWriter, error) {
	dir, err := scope.Dir(f.BaseDir)
	if err != nil {
		return nil, err
	}
	streamDir := filepath.Join(dir, "market")
	if err := os.MkdirAll(streamDir, 0o750); err != nil {
		return nil, fmt.Errorf("create market callback directory: %w", err)
	}
	name := fmt.Sprintf("stream-%s-%06d.ndjson", time.Now().UTC().Format("20060102T150405.000000000Z"), seq)
	path := filepath.Join(streamDir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, fmt.Errorf("create market stream file: %w", err)
	}
	return &activeWriter{path: path, file: file, seq: seq}, nil
}

func (f *MarketFlow) pendingPath(scope Scope, triggerID string) (string, error) {
	dir, err := scope.Dir(f.BaseDir)
	if err != nil {
		return "", err
	}
	pendingDir := filepath.Join(dir, "market", ".pending")
	if err := os.MkdirAll(pendingDir, 0o750); err != nil {
		return "", fmt.Errorf("create market pending directory: %w", err)
	}
	return filepath.Join(pendingDir, deterministicID(triggerID)+".json"), nil
}

func writePending(path string, pending pendingMarketUpload) error {
	data, err := json.Marshal(pending)
	if err != nil {
		return fmt.Errorf("marshal market pending marker: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write market pending marker: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit market pending marker: %w", err)
	}
	return nil
}

func readPending(path string) (*pendingMarketUpload, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pending pendingMarketUpload
	if err := json.Unmarshal(data, &pending); err != nil {
		return nil, fmt.Errorf("decode market pending marker: %w", err)
	}
	return &pending, nil
}

func cleanupPendingMarketUpload(markerPath, segmentPath string) error {
	if segmentPath != "" {
		if err := os.Remove(segmentPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("Drive upload is committed but deleting closed market segment failed: %w", err)
		}
	}
	if err := os.Remove(markerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("Drive upload is committed but deleting pending marker failed: %w", err)
	}
	return nil
}
