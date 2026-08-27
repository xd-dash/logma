package routes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"
)

var (
	client     = redis.NewClient(redisOptionsFromEnv())
	stateStore = newMaraiStateStore(client)

	rootCtx, rootCancel = context.WithCancel(context.Background())
	manager             = newSubscriptionManager()

	httpClient = &http.Client{Timeout: 30 * time.Second}
)

const (
	apiKeyHeader = "X-API-Key"

	callbackQueueSize   = 256
	callbackWorkerCount = 4
	callbackTimeout     = 30 * time.Second
)

var errSubscriptionManagerStopped = errors.New("subscription manager is stopped")

type SubscriptionListener struct {
	Channel   string
	Callback  callbackSecret
	Callbacks []func(callbackSecret, PublishRequest)
}

type PublishRequest struct {
	Type            string      `json:"type"`
	SentTimeUtc     interface{} `json:"sentTimeUtc"`
	Message         interface{} `json:"message"`
	ParentNamespace string      `json:"parentNamespace"`
	ChildNamespace  string      `json:"childNamespace"`
	Channel         string      `json:"channel"`
}

type subscriptionInfo struct {
	ID       string
	Key      string
	Channel  string
	Callback callbackSecret
}

type subscribeResponse struct {
	SubscriptionID string `json:"subscriptionID"`
	Channel        string `json:"channel"`
	CallbackURL    string `json:"callbackURL"`
}

type callbackJob struct {
	callback func(callbackSecret, PublishRequest)
	target   callbackSecret
	message  PublishRequest
}

type callbackDispatcher struct {
	jobs chan callbackJob
	wg   sync.WaitGroup
}

type subscriptionManager struct {
	registerCh   chan registerSubscription
	unregisterCh chan string
	cancelCh     chan cancelSubscription
	snapshotCh   chan chan []subscriptionInfo
	shutdownCh   chan chan struct{}
	done         chan struct{}
}

type managedSubscription struct {
	cancel context.CancelFunc
	info   subscriptionInfo
}

type registerSubscription struct {
	info     subscriptionInfo
	cancel   context.CancelFunc
	response chan error
}

type cancelSubscription struct {
	key      string
	response chan bool
}

func newCallbackDispatcher() *callbackDispatcher {
	d := &callbackDispatcher{jobs: make(chan callbackJob, callbackQueueSize)}
	d.wg.Add(callbackWorkerCount)
	for i := 0; i < callbackWorkerCount; i++ {
		go d.worker()
	}
	return d
}

func (d *callbackDispatcher) worker() {
	defer d.wg.Done()
	for job := range d.jobs {
		job.callback(job.target, job.message)
	}
}

func (d *callbackDispatcher) dispatch(
	callback func(callbackSecret, PublishRequest),
	target callbackSecret,
	message PublishRequest,
) {
	select {
	case d.jobs <- callbackJob{callback: callback, target: target, message: message}:
	default:
		fmt.Println("Callback queue full; dropping callback")
	}
}

func (d *callbackDispatcher) close() {
	close(d.jobs)
	d.wg.Wait()
}

func newSubscriptionManager() *subscriptionManager {
	m := &subscriptionManager{
		registerCh:   make(chan registerSubscription),
		unregisterCh: make(chan string),
		cancelCh:     make(chan cancelSubscription),
		snapshotCh:   make(chan chan []subscriptionInfo),
		shutdownCh:   make(chan chan struct{}),
		done:         make(chan struct{}),
	}
	go m.run()
	return m
}

func (m *subscriptionManager) run() {
	subscriptions := make(map[string]managedSubscription)
	shuttingDown := false
	var shutdownComplete chan struct{}

	defer close(m.done)

	for {
		select {
		case registration := <-m.registerCh:
			if shuttingDown {
				registration.response <- errSubscriptionManagerStopped
				continue
			}
			subscriptions[registration.info.Key] = managedSubscription{
				cancel: registration.cancel,
				info:   registration.info,
			}
			registration.response <- nil

		case key := <-m.unregisterCh:
			delete(subscriptions, key)
			if shuttingDown && len(subscriptions) == 0 {
				if shutdownComplete != nil {
					close(shutdownComplete)
				}
				return
			}

		case request := <-m.cancelCh:
			sub, exists := subscriptions[request.key]
			if !exists {
				request.response <- false
				continue
			}
			sub.cancel()
			request.response <- true

		case response := <-m.snapshotCh:
			snapshot := make([]subscriptionInfo, 0, len(subscriptions))
			for _, sub := range subscriptions {
				snapshot = append(snapshot, sub.info)
			}
			response <- snapshot

		case complete := <-m.shutdownCh:
			if shuttingDown {
				if len(subscriptions) == 0 {
					close(complete)
				}
				continue
			}
			shuttingDown = true
			shutdownComplete = complete
			rootCancel()
			for _, sub := range subscriptions {
				sub.cancel()
			}
			if len(subscriptions) == 0 {
				close(complete)
				return
			}
		}
	}
}

func (m *subscriptionManager) register(info subscriptionInfo, cancel context.CancelFunc) error {
	response := make(chan error, 1)
	select {
	case <-m.done:
		return errSubscriptionManagerStopped
	case m.registerCh <- registerSubscription{info: info, cancel: cancel, response: response}:
		return <-response
	}
}

func (m *subscriptionManager) unregister(key string) {
	select {
	case <-m.done:
	case m.unregisterCh <- key:
	}
}

func (m *subscriptionManager) cancelSubscription(key string) bool {
	response := make(chan bool, 1)
	select {
	case <-m.done:
		return false
	case m.cancelCh <- cancelSubscription{key: key, response: response}:
		return <-response
	}
}

func (m *subscriptionManager) snapshot() []subscriptionInfo {
	response := make(chan []subscriptionInfo, 1)
	select {
	case <-m.done:
		return nil
	case m.snapshotCh <- response:
		return <-response
	}
}

func (m *subscriptionManager) shutdown(ctx context.Context) error {
	complete := make(chan struct{})
	select {
	case <-m.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case m.shutdownCh <- complete:
	}
	select {
	case <-complete:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func authenticateAPIKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := os.Getenv("API_KEY")
		if apiKey == "" {
			http.Error(w, "API key is not set", http.StatusInternalServerError)
			return
		}
		if r.Header.Get(apiKeyHeader) != apiKey {
			http.Error(w, "Invalid API key", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func normalizeCallback(secret callbackSecret) (callbackSecret, error) {
	secret.URL = strings.TrimSpace(secret.URL)
	if secret.URL == "" {
		return callbackSecret{}, errors.New("callback URL is empty")
	}
	if secret.AccessToken != "" && secret.TokenScheme == "" {
		secret.TokenScheme = defaultCallbackTokenScheme
	}
	for _, r := range secret.TokenScheme {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-') {
			return callbackSecret{}, errors.New("invalid token scheme")
		}
	}
	return secret, nil
}

func startSubscription(channelName string, callback callbackSecret) (*subscriptionInfo, error) {
	if channelName == "" {
		return nil, errors.New("channel name is empty")
	}
	var err error
	callback, err = normalizeCallback(callback)
	if err != nil {
		return nil, err
	}

	subCtx, cancel := context.WithCancel(rootCtx)
	if err := subCtx.Err(); err != nil {
		cancel()
		return nil, errSubscriptionManagerStopped
	}

	pubsub := client.Subscribe(subCtx, channelName)
	if _, err := pubsub.Receive(subCtx); err != nil {
		cancel()
		_ = pubsub.Close()
		return nil, fmt.Errorf("subscribe to Redis channel: %w", err)
	}

	subscriptionKey := generateTempChannelID(channelName)
	info := subscriptionInfo{
		ID:       extractSubscriptionID(subscriptionKey),
		Key:      subscriptionKey,
		Channel:  channelName,
		Callback: callback,
	}

	if err := stateStore.saveActive(subCtx, storedSubscription{
		ID: info.ID, Channel: info.Channel, Callback: info.Callback,
	}); err != nil {
		cancel()
		_ = pubsub.Close()
		return nil, err
	}

	if err := manager.register(info, cancel); err != nil {
		cancel()
		_ = stateStore.deleteActive(context.Background(), info.ID)
		_ = pubsub.Close()
		return nil, err
	}

	go runSubscription(subCtx, pubsub, SubscriptionListener{
		Channel:   channelName,
		Callback:  callback,
		Callbacks: []func(callbackSecret, PublishRequest){sendMessageToEndpoint},
	}, &info)

	return &info, nil
}

func runSubscription(
	subCtx context.Context,
	pubsub *redis.PubSub,
	listener SubscriptionListener,
	info *subscriptionInfo,
) {
	dispatcher := newCallbackDispatcher()
	defer dispatcher.close()
	defer manager.unregister(info.Key)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pubsub.Unsubscribe(cleanupCtx, listener.Channel)
		if err := stateStore.deleteActive(cleanupCtx, info.ID); err != nil {
			fmt.Println("Error removing encrypted active subscription state")
		}
		_ = pubsub.Close()
	}()

	for {
		select {
		case <-subCtx.Done():
			return
		case msg, ok := <-pubsub.Channel():
			if !ok {
				return
			}
			if msg.Payload == "" || msg.Payload == "{}" {
				continue
			}
			var message PublishRequest
			if err := json.Unmarshal([]byte(msg.Payload), &message); err != nil {
				fmt.Printf("Error decoding Pub/Sub message: %v\n", err)
				continue
			}
			for _, callback := range listener.Callbacks {
				dispatcher.dispatch(callback, listener.Callback, message)
			}
			if message.Type == "Signal" && message.Message == "UNSUBSCRIBE" {
				return
			}
		}
	}
}

func sendMessageToEndpoint(target callbackSecret, message PublishRequest) {
	payload, err := json.Marshal(message)
	if err != nil {
		fmt.Printf("Error marshaling callback payload: %v\n", err)
		return
	}

	reqCtx, cancel := context.WithTimeout(context.Background(), callbackTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(
		reqCtx,
		http.MethodPost,
		target.URL,
		strings.NewReader(string(payload)),
	)
	if err != nil {
		fmt.Printf("Error creating callback request: %v\n", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if target.AccessToken != "" {
		req.Header.Set("Authorization", target.TokenScheme+" "+target.AccessToken)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf("Error sending callback request: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Printf("Callback endpoint returned HTTP %d\n", resp.StatusCode)
	}
}

func generateTempChannelID(channelName string) string {
	return fmt.Sprintf("active_subscriptions:%d:%s", time.Now().UnixNano(), channelName)
}

func extractSubscriptionID(key string) string {
	parts := strings.SplitN(key, ":", 3)
	if len(parts) != 3 {
		return ""
	}
	return parts[1]
}

func activeChannelsHandler(w http.ResponseWriter, r *http.Request) {
	active := manager.snapshot()
	channels := make([]string, 0, len(active))
	seen := make(map[string]struct{}, len(active))
	for _, info := range active {
		if _, ok := seen[info.Channel]; ok {
			continue
		}
		seen[info.Channel] = struct{}{}
		channels = append(channels, info.Channel)
	}
	sort.Strings(channels)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(channels)
}

func bootstrapHandler(w http.ResponseWriter, r *http.Request) {
	requests := []struct {
		channel  string
		callback callbackSecret
	}{
		{
			channel: "dev:global:logs:rate_limiters",
			callback: callbackSecret{
				URL:         os.Getenv("PUBSUB_DEFAULT_CALLBACK_URL"),
				AccessToken: os.Getenv("PUBSUB_DEFAULT_CALLBACK_TOKEN"),
				TokenScheme: os.Getenv("PUBSUB_DEFAULT_CALLBACK_TOKEN_SCHEME"),
			},
		},
		{
			channel: "dev:global:logs:rate_limiters",
			callback: callbackSecret{
				URL:         os.Getenv("DEFAULT_AXIOM_URL"),
				AccessToken: os.Getenv("DEFAULT_AXIOM_TOKEN"),
				TokenScheme: os.Getenv("DEFAULT_AXIOM_TOKEN_SCHEME"),
			},
		},
		{
			channel: "dev:global:logs:1",
			callback: callbackSecret{
				URL:         os.Getenv("PUBSUB_DEFAULT_CALLBACK_URL"),
				AccessToken: os.Getenv("PUBSUB_DEFAULT_CALLBACK_TOKEN"),
				TokenScheme: os.Getenv("PUBSUB_DEFAULT_CALLBACK_TOKEN_SCHEME"),
			},
		},
	}

	for _, request := range requests {
		if _, err := startSubscription(request.channel, request.callback); err != nil {
			http.Error(w, "Bootstrap subscription failed", http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Bootstrap successful\n"))
}

func NewRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(authenticateAPIKey)

	r.Route("/channels", func(r chi.Router) {
		r.Post("/{channelName}/subscribe", subscribeHandler)
		r.Get("/", activeChannelsHandler)
		r.Post("/groups/save", saveSubscriptionGroup)
		r.Get("/groups/load/{groupID}", loadSubscriptionGroup)
		r.Get("/groups", listSubscriptionGroups)
	})
	r.Get("/bootstrap", bootstrapHandler)
	return r
}

func subscribeHandler(w http.ResponseWriter, r *http.Request) {
	channelName := chi.URLParam(r, "channelName")
	var body struct {
		CallbackURL string `json:"callbackURL"`
		AccessToken string `json:"accessToken,omitempty"`
		TokenScheme string `json:"tokenScheme,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	callback := callbackSecret{
		URL:         body.CallbackURL,
		AccessToken: body.AccessToken,
		TokenScheme: body.TokenScheme,
	}
	if callback.URL == "" {
		callback.URL = os.Getenv("PUBSUB_DEFAULT_CALLBACK_URL")
		if callback.AccessToken == "" {
			callback.AccessToken = os.Getenv("PUBSUB_DEFAULT_CALLBACK_TOKEN")
		}
		if callback.TokenScheme == "" {
			callback.TokenScheme = os.Getenv("PUBSUB_DEFAULT_CALLBACK_TOKEN_SCHEME")
		}
	}

	info, err := startSubscription(channelName, callback)
	if err != nil {
		fmt.Printf("Error starting Redis subscription: %v\n", err)
		http.Error(w, "Error starting Redis subscription", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(subscribeResponse{
		SubscriptionID: info.ID,
		Channel:        info.Channel,
		CallbackURL:    info.Callback.URL,
	})
}

func generateGroupID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func saveSubscriptionGroup(w http.ResponseWriter, r *http.Request) {
	active := manager.snapshot()
	group := storedGroup{
		ID:            generateGroupID(),
		Subscriptions: make([]storedSubscription, 0, len(active)),
	}
	for _, info := range active {
		group.Subscriptions = append(group.Subscriptions, storedSubscription{
			ID: info.ID, Channel: info.Channel, Callback: info.Callback,
		})
	}
	if err := stateStore.saveGroup(rootCtx, group); err != nil {
		fmt.Printf("Error saving encrypted subscription group: %v\n", err)
		http.Error(w, "Error saving subscription group", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte(group.ID))
}

func loadSubscriptionGroup(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")
	group, ok, err := stateStore.loadGroup(rootCtx, groupID)
	if err != nil {
		fmt.Printf("Error loading encrypted subscription group: %v\n", err)
		http.Error(w, "Error retrieving subscription group", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "Subscription group not found", http.StatusNotFound)
		return
	}

	loaded := 0
	for _, sub := range group.Subscriptions {
		if _, err := startSubscription(sub.Channel, sub.Callback); err != nil {
			fmt.Printf("Error loading subscription from group: %v\n", err)
			continue
		}
		loaded++
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"groupID":       groupID,
		"subscriptions": loaded,
		"message":       "Subscription group loaded",
	})
}

func listSubscriptionGroups(w http.ResponseWriter, r *http.Request) {
	cursor := int64(0)
	count := int64(100)

	if raw := r.URL.Query().Get("cursor"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			http.Error(w, "cursor must be a non-negative integer", http.StatusBadRequest)
			return
		}
		cursor = parsed
	}
	if raw := r.URL.Query().Get("count"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 1 || parsed > 1000 {
			http.Error(w, "count must be between 1 and 1000", http.StatusBadRequest)
			return
		}
		count = parsed
	}

	groupIDs, nextCursor, err := stateStore.listGroups(rootCtx, cursor, count)
	if err != nil {
		fmt.Printf("Error listing encrypted subscription groups: %v\n", err)
		http.Error(w, "Error retrieving subscription groups", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"groups":     groupIDs,
		"nextCursor": nextCursor,
	})
}

func stopSubscription(subscriptionKey string) bool {
	return manager.cancelSubscription(subscriptionKey)
}

func ShutdownSubscriptions(ctx context.Context) error {
	return manager.shutdown(ctx)
}

func Shutdown(ctx context.Context) error {
	if err := manager.shutdown(ctx); err != nil {
		return err
	}
	return client.Close()
}
