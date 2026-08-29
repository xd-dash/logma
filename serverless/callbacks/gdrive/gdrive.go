package gdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	defaultTokenURL  = "https://oauth2.googleapis.com/token"
	defaultUploadURL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,mimeType,size,webViewLink,appProperties"
	defaultFilesURL  = "https://www.googleapis.com/drive/v3/files"
	uploadIDProperty = "logma_upload_id"
)

type Config struct {
	ClientID     string
	ClientSecret string
	RefreshToken string
	TokenURL     string
	UploadURL    string
	FilesURL     string
}

type UploadRequest struct {
	Path          string            `json:"path"`
	Name          string            `json:"name,omitempty"`
	FolderID      string            `json:"folder_id,omitempty"`
	MIMEType      string            `json:"mime_type,omitempty"`
	UploadID      string            `json:"upload_id,omitempty"`
	AppProperties map[string]string `json:"app_properties,omitempty"`
}

type UploadResult struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	MIMEType      string            `json:"mimeType"`
	Size          string            `json:"size"`
	WebViewLink   string            `json:"webViewLink"`
	AppProperties map[string]string `json:"appProperties,omitempty"`
	AlreadyExists bool              `json:"already_exists,omitempty"`
}

type Uploader struct {
	config Config
	client *http.Client
}

type Registry struct {
	mu        sync.RWMutex
	uploaders map[string]*Uploader
}

type oauthCredentialFile struct {
	Installed    *oauthClient `json:"installed,omitempty"`
	Web          *oauthClient `json:"web,omitempty"`
	ClientID     string       `json:"client_id,omitempty"`
	ClientSecret string       `json:"client_secret,omitempty"`
	RefreshToken string       `json:"refresh_token,omitempty"`
	TokenURI     string       `json:"token_uri,omitempty"`
}

type oauthClient struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	TokenURI     string `json:"token_uri"`
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Error       string `json:"error"`
	Description string `json:"error_description"`
}

type filesResponse struct {
	Files []UploadResult `json:"files"`
}

func LoadConfig(credentialsPath, refreshToken string) (Config, error) {
	if credentialsPath == "" {
		return Config{}, errors.New("Google Drive credentials path is required")
	}
	data, err := os.ReadFile(credentialsPath)
	if err != nil {
		return Config{}, fmt.Errorf("read Google Drive credentials: %w", err)
	}
	var raw oauthCredentialFile
	if err := json.Unmarshal(data, &raw); err != nil {
		return Config{}, fmt.Errorf("decode Google Drive credentials: %w", err)
	}

	cfg := Config{
		ClientID: raw.ClientID, ClientSecret: raw.ClientSecret,
		RefreshToken: raw.RefreshToken, TokenURL: raw.TokenURI,
		UploadURL: defaultUploadURL, FilesURL: defaultFilesURL,
	}
	if raw.Installed != nil {
		cfg.ClientID, cfg.ClientSecret, cfg.TokenURL = raw.Installed.ClientID, raw.Installed.ClientSecret, raw.Installed.TokenURI
	} else if raw.Web != nil {
		cfg.ClientID, cfg.ClientSecret, cfg.TokenURL = raw.Web.ClientID, raw.Web.ClientSecret, raw.Web.TokenURI
	}
	if refreshToken != "" {
		cfg.RefreshToken = refreshToken
	}
	if cfg.TokenURL == "" {
		cfg.TokenURL = defaultTokenURL
	}
	if cfg.ClientID == "" || cfg.ClientSecret == "" || cfg.RefreshToken == "" {
		return Config{}, errors.New("Google Drive client_id, client_secret, and refresh token are required")
	}
	return cfg, nil
}

func New(cfg Config) *Uploader {
	if cfg.TokenURL == "" {
		cfg.TokenURL = defaultTokenURL
	}
	if cfg.UploadURL == "" {
		cfg.UploadURL = defaultUploadURL
	}
	if cfg.FilesURL == "" {
		cfg.FilesURL = defaultFilesURL
	}
	return &Uploader{config: cfg, client: &http.Client{Timeout: 2 * time.Minute}}
}

func NewFromEnv() (*Uploader, error) {
	cfg, err := LoadConfig(os.Getenv("GOOGLE_DRIVE_CREDENTIALS_FILE"), os.Getenv("GOOGLE_DRIVE_REFRESH_TOKEN"))
	if err != nil {
		return nil, err
	}
	return New(cfg), nil
}

func NewRegistry() *Registry { return &Registry{uploaders: make(map[string]*Uploader)} }

func (r *Registry) Register(credentialID string, uploader *Uploader) error {
	credentialID = strings.TrimSpace(credentialID)
	if credentialID == "" {
		return errors.New("Google Drive credential ID is required")
	}
	if uploader == nil {
		return errors.New("Google Drive uploader is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.uploaders[credentialID] = uploader
	return nil
}

func (r *Registry) Resolve(credentialID string) (*Uploader, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	uploader := r.uploaders[credentialID]
	if uploader == nil {
		return nil, fmt.Errorf("Google Drive credential %q is not registered", credentialID)
	}
	return uploader, nil
}

func (u *Uploader) Upload(ctx context.Context, req UploadRequest) (UploadResult, error) {
	if req.Path == "" {
		return UploadResult{}, errors.New("upload path is required")
	}
	if req.UploadID != "" {
		existing, ok, err := u.FindByUploadID(ctx, req.UploadID)
		if err != nil {
			return UploadResult{}, err
		}
		if ok {
			existing.AlreadyExists = true
			return existing, nil
		}
	}

	f, err := os.Open(req.Path)
	if err != nil {
		return UploadResult{}, fmt.Errorf("open upload file: %w", err)
	}
	defer f.Close()

	name := req.Name
	if name == "" {
		name = filepath.Base(req.Path)
	}
	mimeType := req.MIMEType
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	accessToken, err := u.accessToken(ctx)
	if err != nil {
		return UploadResult{}, err
	}

	properties := make(map[string]string, len(req.AppProperties)+1)
	for k, v := range req.AppProperties {
		properties[k] = v
	}
	if req.UploadID != "" {
		properties[uploadIDProperty] = req.UploadID
	}
	metadata := map[string]any{"name": name}
	if req.FolderID != "" {
		metadata["parents"] = []string{req.FolderID}
	}
	if len(properties) > 0 {
		metadata["appProperties"] = properties
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return UploadResult{}, fmt.Errorf("marshal Drive metadata: %w", err)
	}

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	contentType := "multipart/related; boundary=" + mw.Boundary()
	go func() {
		header := make(textproto.MIMEHeader)
		header.Set("Content-Type", "application/json; charset=UTF-8")
		part, writeErr := mw.CreatePart(header)
		if writeErr == nil {
			_, writeErr = part.Write(metadataJSON)
		}
		if writeErr == nil {
			header = make(textproto.MIMEHeader)
			header.Set("Content-Type", mimeType)
			part, writeErr = mw.CreatePart(header)
		}
		if writeErr == nil {
			_, writeErr = io.Copy(part, f)
		}
		if closeErr := mw.Close(); writeErr == nil {
			writeErr = closeErr
		}
		if writeErr != nil {
			_ = pw.CloseWithError(writeErr)
			return
		}
		_ = pw.Close()
	}()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.config.UploadURL, pr)
	if err != nil {
		return UploadResult{}, fmt.Errorf("create Drive upload request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+accessToken)
	httpReq.Header.Set("Content-Type", contentType)

	resp, err := u.client.Do(httpReq)
	if err != nil {
		return UploadResult{}, fmt.Errorf("upload to Google Drive: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return UploadResult{}, fmt.Errorf("read Google Drive response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return UploadResult{}, fmt.Errorf("Google Drive upload failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var result UploadResult
	if err := json.Unmarshal(body, &result); err != nil {
		return UploadResult{}, fmt.Errorf("decode Google Drive upload response: %w", err)
	}
	return result, nil
}

func (u *Uploader) FindByUploadID(ctx context.Context, uploadID string) (UploadResult, bool, error) {
	if strings.TrimSpace(uploadID) == "" {
		return UploadResult{}, false, nil
	}
	accessToken, err := u.accessToken(ctx)
	if err != nil {
		return UploadResult{}, false, err
	}
	q := fmt.Sprintf("appProperties has { key='%s' and value='%s' } and trashed=false", uploadIDProperty, driveQueryLiteral(uploadID))
	endpoint, err := url.Parse(u.config.FilesURL)
	if err != nil {
		return UploadResult{}, false, fmt.Errorf("parse Google Drive files URL: %w", err)
	}
	values := endpoint.Query()
	values.Set("q", q)
	values.Set("pageSize", "1")
	values.Set("fields", "files(id,name,mimeType,size,webViewLink,appProperties)")
	endpoint.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return UploadResult{}, false, fmt.Errorf("create Google Drive lookup request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := u.client.Do(req)
	if err != nil {
		return UploadResult{}, false, fmt.Errorf("query Google Drive upload ID: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return UploadResult{}, false, fmt.Errorf("read Google Drive lookup response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return UploadResult{}, false, fmt.Errorf("Google Drive lookup failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var result filesResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return UploadResult{}, false, fmt.Errorf("decode Google Drive lookup response: %w", err)
	}
	if len(result.Files) == 0 {
		return UploadResult{}, false, nil
	}
	return result.Files[0], true, nil
}

func (u *Uploader) Handle(ctx context.Context, payload string) error {
	var req UploadRequest
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		return fmt.Errorf("decode Google Drive callback payload: %w", err)
	}
	_, err := u.Upload(ctx, req)
	return err
}

func (u *Uploader) accessToken(ctx context.Context) (string, error) {
	form := url.Values{
		"client_id":     {u.config.ClientID},
		"client_secret": {u.config.ClientSecret},
		"refresh_token": {u.config.RefreshToken},
		"grant_type":    {"refresh_token"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.config.TokenURL, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("create OAuth refresh request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("refresh Google OAuth token: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read OAuth refresh response: %w", err)
	}
	var token tokenResponse
	if err := json.Unmarshal(body, &token); err != nil {
		return "", fmt.Errorf("decode OAuth refresh response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || token.AccessToken == "" {
		return "", fmt.Errorf("Google OAuth refresh failed: status=%d error=%s description=%s", resp.StatusCode, token.Error, token.Description)
	}
	return token.AccessToken, nil
}

func driveQueryLiteral(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `'`, `\'`)
}
