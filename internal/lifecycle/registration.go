package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
)

// Registration is durable lifecycle intent. Redis may cache the active timer,
// but this record preserves the original activation time and encoded policy so
// a reboot can reconstruct the same absolute deadline instead of extending it.
type Registration struct {
	DeploymentID    string                          `json:"deployment_id"`
	PolicyCode      string                          `json:"policy_code"`
	PolicyName      ratelimiter.LifecyclePolicyName `json:"policy_name,omitempty"`
	ActivatedAt     time.Time                       `json:"activated_at"`
	Deadline        time.Time                       `json:"deadline"`
	ShutdownChannel string                          `json:"shutdown_channel"`
	Metadata        map[string]string               `json:"metadata,omitempty"`
}

type RegisterRequest struct {
	DeploymentID    string                          `json:"deployment_id"`
	PolicyCode      string                          `json:"policy_code,omitempty"`
	PolicyName      ratelimiter.LifecyclePolicyName `json:"policy_name,omitempty"`
	ActivatedAt     *time.Time                      `json:"activated_at,omitempty"`
	ShutdownChannel string                          `json:"shutdown_channel"`
	Metadata        map[string]string               `json:"metadata,omitempty"`
}

func NewRegistration(req RegisterRequest, now time.Time) (Registration, error) {
	if strings.TrimSpace(req.DeploymentID) == "" {
		return Registration{}, errors.New("deployment_id is required")
	}
	if strings.ContainsAny(req.DeploymentID, "/\\\x00") {
		return Registration{}, errors.New("deployment_id contains an invalid path character")
	}
	if strings.TrimSpace(req.ShutdownChannel) == "" {
		return Registration{}, errors.New("shutdown_channel is required")
	}

	code, err := policyCodeForRequest(req)
	if err != nil {
		return Registration{}, err
	}

	activated := now.UTC()
	if req.ActivatedAt != nil {
		activated = req.ActivatedAt.UTC()
	}
	deadline, err := ratelimiter.LifecycleDeadline(activated, code)
	if err != nil {
		return Registration{}, err
	}

	return Registration{
		DeploymentID:    req.DeploymentID,
		PolicyCode:      strconv.FormatUint(uint64(code), 10),
		PolicyName:      req.PolicyName,
		ActivatedAt:     activated,
		Deadline:        deadline,
		ShutdownChannel: req.ShutdownChannel,
		Metadata:        cloneMetadata(req.Metadata),
	}, nil
}

func policyCodeForRequest(req RegisterRequest) (ratelimiter.PolicyCode, error) {
	if req.PolicyCode != "" {
		raw, err := strconv.ParseUint(req.PolicyCode, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse policy_code: %w", err)
		}
		code := ratelimiter.PolicyCode(raw)
		if _, err := ratelimiter.DecodePolicy(code); err != nil {
			return 0, fmt.Errorf("decode policy_code: %w", err)
		}
		return code, nil
	}
	if req.PolicyName == "" {
		return 0, errors.New("policy_code or policy_name is required")
	}
	policy, err := ratelimiter.NamedLifecyclePolicy(req.PolicyName)
	if err != nil {
		return 0, err
	}
	code, err := ratelimiter.EncodePolicy(policy)
	if err != nil {
		return 0, err
	}
	return code, nil
}

func (r Registration) Validate() error {
	if r.DeploymentID == "" || r.PolicyCode == "" || r.ShutdownChannel == "" {
		return errors.New("registration is incomplete")
	}
	raw, err := strconv.ParseUint(r.PolicyCode, 10, 64)
	if err != nil {
		return fmt.Errorf("parse stored policy_code: %w", err)
	}
	deadline, err := ratelimiter.LifecycleDeadline(r.ActivatedAt, ratelimiter.PolicyCode(raw))
	if err != nil {
		return err
	}
	if !deadline.Equal(r.Deadline) {
		return fmt.Errorf("stored deadline %s does not match policy deadline %s", r.Deadline, deadline)
	}
	return nil
}

// MatchesRequest determines whether a repeated handoff is the same durable
// intent. Omitted activated_at means "use the already-persisted activation" for
// retry purposes; callers cannot use omission to rebase an existing lifecycle.
func (r Registration) MatchesRequest(req RegisterRequest) (bool, error) {
	if req.DeploymentID != r.DeploymentID || req.ShutdownChannel != r.ShutdownChannel {
		return false, nil
	}
	code, err := policyCodeForRequest(req)
	if err != nil {
		return false, err
	}
	if strconv.FormatUint(uint64(code), 10) != r.PolicyCode {
		return false, nil
	}
	if req.ActivatedAt != nil && !req.ActivatedAt.UTC().Equal(r.ActivatedAt) {
		return false, nil
	}
	return reflect.DeepEqual(cloneMetadata(req.Metadata), cloneMetadata(r.Metadata)), nil
}

type FileStore struct {
	Dir string
}

func (s FileStore) Save(reg Registration) error {
	return s.write(reg, false)
}

// Create persists a new deployment lifecycle without overwriting an existing
// registration. The hard-link publication step is atomic on the state directory
// filesystem and gives concurrent handoff retries one winner.
func (s FileStore) Create(reg Registration) error {
	return s.write(reg, true)
}

func (s FileStore) write(reg Registration, createOnly bool) error {
	if err := reg.Validate(); err != nil {
		return err
	}
	if s.Dir == "" {
		return errors.New("lifecycle state directory is empty")
	}
	if err := os.MkdirAll(s.Dir, 0700); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.Dir, ".registration-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(payload, '\n')); err != nil {
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

	target := s.path(reg.DeploymentID)
	if createOnly {
		if err := os.Link(tmpName, target); err != nil {
			return err
		}
		return nil
	}
	return os.Rename(tmpName, target)
}

func (s FileStore) Load(deploymentID string) (Registration, error) {
	payload, err := os.ReadFile(s.path(deploymentID))
	if err != nil {
		return Registration{}, err
	}
	var reg Registration
	if err := json.Unmarshal(payload, &reg); err != nil {
		return Registration{}, fmt.Errorf("decode lifecycle registration: %w", err)
	}
	if err := reg.Validate(); err != nil {
		return Registration{}, fmt.Errorf("validate lifecycle registration: %w", err)
	}
	return reg, nil
}

func (s FileStore) LoadAll() ([]Registration, error) {
	entries, err := os.ReadDir(s.Dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	regs := make([]Registration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		reg, err := s.Load(strings.TrimSuffix(entry.Name(), ".json"))
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", entry.Name(), err)
		}
		regs = append(regs, reg)
	}
	return regs, nil
}

func (s FileStore) Delete(deploymentID string) error {
	err := os.Remove(s.path(deploymentID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s FileStore) path(deploymentID string) string {
	return filepath.Join(s.Dir, deploymentID+".json")
}

func cloneMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
