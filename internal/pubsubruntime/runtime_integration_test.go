package pubsubruntime

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/internal/pubsubmodel"
)

func TestRuntimeActivatesEmptyChannelAgainstRedis(t *testing.T) {
	addr := os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
	if addr == "" {
		t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}

	const scope = "huram-local-channel-runtime"
	const channelName = "events-empty"
	store, err := pubsubmodel.NewRedisStore(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutChannel(ctx, pubsubmodel.Channel{Name: channelName}); err != nil {
		t.Fatalf("put Channel: %v", err)
	}

	runtime, err := New(client, store)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := runtime.Activate(ctx, channelName, nil)
	if err != nil {
		t.Fatalf("activate Channel: %v", err)
	}

	select {
	case <-handle.Ready():
	case <-ctx.Done():
		t.Fatalf("wait for Channel readiness: %v; last Redis error: %v", ctx.Err(), handle.LastError())
	}

	numsub, err := client.PubSubNumSub(ctx, channelName).Result()
	if err != nil {
		t.Fatalf("PUBSUB NUMSUB: %v", err)
	}
	if numsub[channelName] != 1 {
		t.Fatalf("PUBSUB NUMSUB %s = %d, want 1", channelName, numsub[channelName])
	}

	if !handle.Close() {
		t.Fatal("Close did not deactivate Channel")
	}
	select {
	case <-handle.Stopped():
	case <-ctx.Done():
		t.Fatalf("wait for Channel stop: %v", ctx.Err())
	}

	if runtime.Active(channelName) {
		t.Fatal("Channel remains active after Close")
	}
	if _, err := store.GetChannel(ctx, channelName); err != nil {
		t.Fatalf("persisted Channel was removed by runtime deactivation: %v", err)
	}

	numsub, err = client.PubSubNumSub(ctx, channelName).Result()
	if err != nil {
		t.Fatalf("PUBSUB NUMSUB after close: %v", err)
	}
	if numsub[channelName] != 0 {
		t.Fatalf("PUBSUB NUMSUB %s after close = %d, want 0", channelName, numsub[channelName])
	}

	if err := store.DeleteChannel(ctx, channelName); err != nil {
		t.Fatalf("delete Channel: %v", err)
	}
}

func TestRuntimeDeliversPersistedSubscriberWebhooksAgainstRedis(t *testing.T) {
	addr := os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
	if addr == "" {
		t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}

	deliveries := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		deliveries <- r.URL.Path + "|" + string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	const scope = "huram-local-subscriber-runtime"
	const channelName = "events-webhook"
	const callbackID = "webhook-a"
	const subscriberID = "subscriber-a"
	store, err := pubsubmodel.NewRedisStore(client, scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutChannel(ctx, pubsubmodel.Channel{Name: channelName}); err != nil {
		t.Fatalf("put Channel: %v", err)
	}
	if err := store.PutCallback(ctx, pubsubmodel.Callback{
		ID:   callbackID,
		Type: pubsubmodel.CallbackWebhook,
		Webhook: &pubsubmodel.WebhookCallback{CallbackURLs: []string{
			server.URL + "/one",
			server.URL + "/two",
		}},
	}); err != nil {
		t.Fatalf("put Callback: %v", err)
	}
	if err := store.PutSubscriber(ctx, pubsubmodel.Subscriber{
		ID:          subscriberID,
		Channel:     channelName,
		CallbackIDs: []string{callbackID},
	}); err != nil {
		t.Fatalf("put Subscriber: %v", err)
	}

	runtime, err := New(client, store)
	if err != nil {
		t.Fatal(err)
	}
	channelHandle, err := runtime.Activate(ctx, channelName, nil)
	if err != nil {
		t.Fatalf("activate Channel: %v", err)
	}
	select {
	case <-channelHandle.Ready():
	case <-ctx.Done():
		t.Fatalf("wait for Channel readiness: %v; last Redis error: %v", ctx.Err(), channelHandle.LastError())
	}

	numsub, err := client.PubSubNumSub(ctx, channelName).Result()
	if err != nil {
		t.Fatalf("PUBSUB NUMSUB: %v", err)
	}
	if numsub[channelName] != 1 {
		t.Fatalf("PUBSUB NUMSUB %s = %d, want exactly one Channel listener", channelName, numsub[channelName])
	}

	subscriberHandle, err := runtime.AttachSubscriber(ctx, subscriberID)
	if err != nil {
		t.Fatalf("attach Subscriber: %v", err)
	}

	const payload = `{"probe":"subscriber-runtime"}`
	if err := client.Publish(ctx, channelName, payload).Err(); err != nil {
		t.Fatalf("publish: %v", err)
	}

	got := make([]string, 0, 2)
	for len(got) < 2 {
		select {
		case delivery := <-deliveries:
			got = append(got, delivery)
		case <-ctx.Done():
			t.Fatalf("wait for webhook deliveries: %v; got %v", ctx.Err(), got)
		}
	}
	sort.Strings(got)
	want := []string{
		"/one|" + payload,
		"/two|" + payload,
	}
	if got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("webhook deliveries = %v, want %v", got, want)
	}

	if !subscriberHandle.Close() {
		t.Fatal("Subscriber Close did not detach runtime delivery")
	}
	if err := client.Publish(ctx, channelName, `{"probe":"detached"}`).Err(); err != nil {
		t.Fatalf("publish after detach: %v", err)
	}
	select {
	case delivery := <-deliveries:
		t.Fatalf("detached Subscriber received webhook delivery %q", delivery)
	case <-time.After(150 * time.Millisecond):
	}

	if _, err := store.GetSubscriber(ctx, subscriberID); err != nil {
		t.Fatalf("runtime detach removed persisted Subscriber: %v", err)
	}
	if _, err := store.GetCallback(ctx, callbackID); err != nil {
		t.Fatalf("runtime detach removed persisted Callback: %v", err)
	}

	if !channelHandle.Close() {
		t.Fatal("Channel Close did not deactivate listener")
	}
	select {
	case <-channelHandle.Stopped():
	case <-ctx.Done():
		t.Fatalf("wait for Channel stop: %v", ctx.Err())
	}

	if err := store.DeleteSubscriber(ctx, subscriberID); err != nil {
		t.Fatalf("delete Subscriber: %v", err)
	}
	if err := store.DeleteCallback(ctx, callbackID); err != nil {
		t.Fatalf("delete Callback: %v", err)
	}
	if err := store.DeleteChannel(ctx, channelName); err != nil {
		t.Fatalf("delete Channel: %v", err)
	}
}
