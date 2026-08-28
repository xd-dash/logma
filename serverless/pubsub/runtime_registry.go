package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	runtimeLeaseTTL       = 90 * time.Second
	runtimeHeartbeatEvery = 30 * time.Second
)

// SubscriptionDescriptor is the persistable identity of a runtime subscription.
// Callback names describe behavior; executable Go callbacks remain process-local.
type SubscriptionDescriptor struct {
	ID       string `json:"id"`
	Channel  string `json:"channel"`
	Callback string `json:"callback"`
}

// RuntimeRecord is the discoverable control-plane state for one live request runtime.
type RuntimeRecord struct {
	Key             string                   `json:"key"`
	Namespace       string                   `json:"namespace"`
	InstanceID      string                   `json:"instance_id"`
	RequestID       string                   `json:"request_id"`
	InvocationKey   string                   `json:"invocation_key"`
	ShutdownChannel string                   `json:"shutdown_channel"`
	StartedAt       time.Time                `json:"started_at"`
	UpdatedAt       time.Time                `json:"updated_at"`
	Subscriptions   []SubscriptionDescriptor `json:"subscriptions"`
}

func RuntimeRecordKey(namespace, instanceID, requestID string) string {
	return fmt.Sprintf("runtime:%s:%s:%s", cleanPart(namespace), cleanPart(instanceID), cleanPart(requestID))
}

func runtimeIndexKey(namespace string) string {
	return "runtime_index:" + cleanPart(namespace)
}

func cleanPart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return strings.ReplaceAll(value, " ", "_")
}

func normalizeDescriptors(handlers ChannelHandlers, descriptors []SubscriptionDescriptor) []SubscriptionDescriptor {
	byChannel := make(map[string]SubscriptionDescriptor, len(descriptors))
	for _, descriptor := range descriptors {
		if descriptor.Channel == "" {
			continue
		}
		if descriptor.ID == "" {
			descriptor.ID = cleanPart(descriptor.Channel)
		}
		if descriptor.Callback == "" {
			descriptor.Callback = "internal:handler"
		}
		byChannel[descriptor.Channel] = descriptor
	}
	for channel := range handlers {
		if _, ok := byChannel[channel]; ok {
			continue
		}
		byChannel[channel] = SubscriptionDescriptor{
			ID:       cleanPart(channel),
			Channel:  channel,
			Callback: "internal:handler",
		}
	}
	result := make([]SubscriptionDescriptor, 0, len(byChannel))
	for _, descriptor := range byChannel {
		result = append(result, descriptor)
	}
	return result
}

type RuntimeLease struct {
	client    *redis.Client
	record    RuntimeRecord
	indexKey  string
	stop      chan struct{}
	closeOnce sync.Once
}

func (cp ControlPlane) RegisterRuntime(ctx context.Context, invocation InvocationInfo, subscriptions []SubscriptionDescriptor) (*RuntimeLease, error) {
	namespace := cp.Namespace
	if namespace == "" {
		namespace = invocation.Service
	}
	if namespace == "" {
		namespace = "service"
	}
	startedAt := invocation.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	now := time.Now().UTC()
	record := RuntimeRecord{
		Namespace:       namespace,
		InstanceID:      cp.InstanceID,
		RequestID:       invocation.RequestID,
		InvocationKey:   InvocationKey(invocation),
		ShutdownChannel: cp.InstanceChannel(cp.ShutdownChannel()),
		StartedAt:       startedAt,
		UpdatedAt:       now,
		Subscriptions:   subscriptions,
	}
	record.Key = RuntimeRecordKey(record.Namespace, record.InstanceID, record.RequestID)
	encodedSubscriptions, err := json.Marshal(record.Subscriptions)
	if err != nil {
		return nil, fmt.Errorf("marshal runtime subscriptions: %w", err)
	}
	fields := map[string]any{
		"namespace": record.Namespace,
		"instance_id": record.InstanceID,
		"request_id": record.RequestID,
		"invocation_key": record.InvocationKey,
		"shutdown_channel": record.ShutdownChannel,
		"started_at": record.StartedAt.Format(time.RFC3339Nano),
		"updated_at": record.UpdatedAt.Format(time.RFC3339Nano),
		"subscriptions": string(encodedSubscriptions),
	}
	indexKey := runtimeIndexKey(record.Namespace)
	pipe := cp.Client.TxPipeline()
	pipe.HSet(ctx, record.Key, fields)
	pipe.Expire(ctx, record.Key, runtimeLeaseTTL)
	pipe.SAdd(ctx, indexKey, record.Key)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("register runtime %s: %w", record.Key, err)
	}
	lease := &RuntimeLease{client: cp.Client, record: record, indexKey: indexKey, stop: make(chan struct{})}
	go lease.heartbeat(ctx)
	return lease, nil
}

func (lease *RuntimeLease) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(runtimeHeartbeatEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-lease.stop:
			return
		case now := <-ticker.C:
			lease.record.UpdatedAt = now.UTC()
			pipe := lease.client.TxPipeline()
			pipe.HSet(ctx, lease.record.Key, "updated_at", lease.record.UpdatedAt.Format(time.RFC3339Nano))
			pipe.Expire(ctx, lease.record.Key, runtimeLeaseTTL)
			_, _ = pipe.Exec(ctx)
		}
	}
}

func (lease *RuntimeLease) Close() error {
	var closeErr error
	lease.closeOnce.Do(func() {
		close(lease.stop)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pipe := lease.client.TxPipeline()
		pipe.Del(ctx, lease.record.Key)
		pipe.SRem(ctx, lease.indexKey, lease.record.Key)
		_, closeErr = pipe.Exec(ctx)
	})
	return closeErr
}

func LoadRuntimeRecord(ctx context.Context, client *redis.Client, key string) (RuntimeRecord, error) {
	fields, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		return RuntimeRecord{}, err
	}
	if len(fields) == 0 {
		return RuntimeRecord{}, redis.Nil
	}
	record := RuntimeRecord{
		Key:             key,
		Namespace:       fields["namespace"],
		InstanceID:      fields["instance_id"],
		RequestID:       fields["request_id"],
		InvocationKey:   fields["invocation_key"],
		ShutdownChannel: fields["shutdown_channel"],
	}
	if value := fields["started_at"]; value != "" {
		record.StartedAt, _ = time.Parse(time.RFC3339Nano, value)
	}
	if value := fields["updated_at"]; value != "" {
		record.UpdatedAt, _ = time.Parse(time.RFC3339Nano, value)
	}
	if value := fields["subscriptions"]; value != "" {
		if err := json.Unmarshal([]byte(value), &record.Subscriptions); err != nil {
			return RuntimeRecord{}, fmt.Errorf("decode runtime subscriptions: %w", err)
		}
	}
	return record, nil
}

func ListRuntimeRecords(ctx context.Context, client *redis.Client, namespace string) ([]RuntimeRecord, error) {
	indexKey := runtimeIndexKey(namespace)
	keys, err := client.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, err
	}
	records := make([]RuntimeRecord, 0, len(keys))
	for _, key := range keys {
		record, err := LoadRuntimeRecord(ctx, client, key)
		if errors.Is(err, redis.Nil) {
			_ = client.SRem(ctx, indexKey, key).Err()
			continue
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

// SaveRuntimeSubscriptionGroup snapshots the live runtime's persistable
// channel/callback descriptors into Logma's existing subscription-group key shape.
func SaveRuntimeSubscriptionGroup(ctx context.Context, client *redis.Client, runtimeKey, groupID string) error {
	record, err := LoadRuntimeRecord(ctx, client, runtimeKey)
	if err != nil {
		return err
	}
	if strings.TrimSpace(groupID) == "" {
		return errors.New("group id is empty")
	}
	pipe := client.TxPipeline()
	for i, subscription := range record.Subscriptions {
		id := subscription.ID
		if id == "" {
			id = fmt.Sprintf("runtime-%d", i+1)
		}
		key := fmt.Sprintf("subscription_groups:%s:%s:%s", groupID, id, subscription.Channel)
		pipe.Set(ctx, key, subscription.Callback, 0)
	}
	_, err = pipe.Exec(ctx)
	return err
}

// ShutdownRuntime addresses one live runtime directly through its instance
// shutdown channel. The runtime removes its lease when cancellation completes.
func ShutdownRuntime(ctx context.Context, client *redis.Client, record RuntimeRecord, reason string) error {
	if record.ShutdownChannel == "" {
		return errors.New("runtime has no shutdown channel")
	}
	payload, err := json.Marshal(ShutdownRequest{Reason: reason})
	if err != nil {
		return err
	}
	return client.Publish(ctx, record.ShutdownChannel, payload).Err()
}
