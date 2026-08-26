package routes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/redis-utils/callbacks"
	"github.com/xd-dash/redis-utils/scanner"
)

var (
	rootCtx, rootCancel = context.WithCancel(context.Background())
	manager             = newSubscriptionManager()
	tenantClients       = newTenantClientRegistry()
	httpClient          = &http.Client{Timeout: 30 * time.Second}
)

const (
	apiKeyHeader        = "X-API-Key"
	callbackQueueSize   = 256
	callbackWorkerCount = 4
	callbackTimeout     = 30 * time.Second
)

var errSubscriptionManagerStopped = errors.New("subscription manager is stopped")

type SubscriptionListener struct {
	Channel     string
	CallbackURL string
	Callbacks   []func(url string, message PublishRequest)
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
	TenantID    string `json:"tenantID"`
	ID          string `json:"subscriptionID"`
	Key         string `json:"-"`
	Channel     string `json:"channel"`
	CallbackURL string `json:"callbackURL"`
}

type saveGroupRequest struct {
	SubscriptionIDs []string `json:"subscriptionIDs"`
}

type callbackJob struct {
	callback func(url string, message PublishRequest)
	url      string
	message  PublishRequest
}

type callbackDispatcher struct {
	jobs chan callbackJob
	wg   sync.WaitGroup
}

type subscriptionManager struct {
	register   chan registerSubscription
	unregister chan string
	cancel     chan cancelSubscription
	shutdown   chan chan struct{}
	done       chan struct{}
}

type registerSubscription struct {
	key      string
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
		job.callback(job.url, job.message)
	}
}

func (d *callbackDispatcher) dispatch(callback func(string, PublishRequest), url string, message PublishRequest) {
	select {
	case d.jobs <- callbackJob{callback: callback, url: url, message: message}:
	default:
		fmt.Printf("Callback queue full; dropping callback for %s\n", url)
	}
}

func (d *callbackDispatcher) close() {
	close(d.jobs)
	d.wg.Wait()
}

func newSubscriptionManager() *subscriptionManager {
	m := &subscriptionManager{
		register:   make(chan registerSubscription),
		unregister: make(chan string),
		cancel:     make(chan cancelSubscription),
		shutdown:   make(chan chan struct{}),
		done:       make(chan struct{}),
	}
	go m.run()
	return m
}

func (m *subscriptionManager) run() {
	subscriptions := make(map[string]context.CancelFunc)
	active := 0
	shuttingDown := false
	var shutdownComplete chan struct{}
	defer close(m.done)

	for {
		select {
		case registration := <-m.register:
			if shuttingDown {
				registration.response <- errSubscriptionManagerStopped
				continue
			}
			if _, exists := subscriptions[registration.key]; !exists {
				active++
			}
			subscriptions[registration.key] = registration.cancel
			registration.response <- nil

		case key := <-m.unregister:
			if _, exists := subscriptions[key]; !exists {
				continue
			}
			delete(subscriptions, key)
			active--
			if shuttingDown && active == 0 {
				if shutdownComplete != nil {
					close(shutdownComplete)
				}
				return
			}

		case request := <-m.cancel:
			cancel, exists := subscriptions[request.key]
			if exists {
				cancel()
			}
			request.response <- exists

		case complete := <-m.shutdown:
			if shuttingDown {
				if active == 0 {
					close(complete)
				}
				continue
			}
			shuttingDown = true
			shutdownComplete = complete
			rootCancel()
			for _, cancel := range subscriptions {
				cancel()
			}
			if active == 0 {
				close(shutdownComplete)
				return
			}
		}
	}
}

func (m *subscriptionManager) registerSubscription(key string, cancel context.CancelFunc) error {
	response := make(chan error, 1)
	select {
	case <-m.done:
		return errSubscriptionManagerStopped
	case m.register <- registerSubscription{key: key, cancel: cancel, response: response}:
		return <-response
	}
}

func (m *subscriptionManager) unregisterSubscription(key string) {
	select {
	case <-m.done:
	case m.unregister <- key:
	}
}

func (m *subscriptionManager) cancelSubscription(key string) bool {
	response := make(chan bool, 1)
	select {
	case <-m.done:
		return false
	case m.cancel <- cancelSubscription{key: key, response: response}:
		return <-response
	}
}

func (m *subscriptionManager) shutdownSubscriptions(ctx context.Context) error {
	complete := make(chan struct{})
	select {
	case <-m.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case m.shutdown <- complete:
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

func requestTenantScope(w http.ResponseWriter, r *http.Request) (tenantScope, bool) {
	scope, err := tenantFromRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return tenantScope{}, false
	}
	return scope, true
}

func startSubscription(scope tenantScope, channelName, callbackURL string) (*subscriptionInfo, error) {
	if channelName == "" {
		return nil, errors.New("channel name is empty")
	}
	if callbackURL == "" {
		return nil, errors.New("callback URL is empty")
	}

	subCtx, cancel := context.WithCancel(rootCtx)
	if err := subCtx.Err(); err != nil {
		cancel()
		return nil, errSubscriptionManagerStopped
	}

	pubsub := scope.Client.Subscribe(subCtx, channelName)
	if _, err := pubsub.Receive(subCtx); err != nil {
		cancel()
		_ = pubsub.Close()
		return nil, fmt.Errorf("subscribe to Redis channel %q: %w", channelName, err)
	}

	subscriptionKey := activeSubscriptionPattern(scope.ID, fmt.Sprintf("%d", time.Now().UnixNano()), channelName)
	info, err := subscriptionFromKeyParts(subscriptionKey, callbackURL)
	if err != nil {
		cancel()
		_ = pubsub.Close()
		return nil, err
	}

	if err := scope.Client.Set(subCtx, subscriptionKey, callbackURL, 0).Err(); err != nil {
		cancel()
		_ = pubsub.Close()
		return nil, fmt.Errorf("save subscription state for %q: %w", channelName, err)
	}
	if err := manager.registerSubscription(subscriptionKey, cancel); err != nil {
		cancel()
		_ = scope.Client.Del(context.Background(), subscriptionKey).Err()
		_ = pubsub.Close()
		return nil, err
	}

	go runSubscription(scope.Client, subCtx, pubsub, SubscriptionListener{
		Channel:     channelName,
		CallbackURL: callbackURL,
		Callbacks:   []func(string, PublishRequest){sendMessageToEndpoint},
	}, &info)
	return &info, nil
}

func runSubscription(redisClient *redis.Client, subCtx context.Context, pubsub *redis.PubSub, listener SubscriptionListener, info *subscriptionInfo) {
	dispatcher := newCallbackDispatcher()
	defer dispatcher.close()
	defer manager.unregisterSubscription(info.Key)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pubsub.Unsubscribe(cleanupCtx, listener.Channel)
		_ = redisClient.Del(cleanupCtx, info.Key).Err()
		_ = pubsub.Close()
	}()

	messages := pubsub.Channel()
	for {
		select {
		case <-subCtx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			if msg.Payload == "" || msg.Payload == "{}" {
				continue
			}
			var message PublishRequest
			if err := json.Unmarshal([]byte(msg.Payload), &message); err != nil {
				fmt.Printf("Error decoding message: %v\n", err)
				continue
			}
			for _, callback := range listener.Callbacks {
				dispatcher.dispatch(callback, listener.CallbackURL, message)
			}
			if message.Type == "Signal" && message.Message == "UNSUBSCRIBE" {
				return
			}
		}
	}
}

func sendMessageToEndpoint(url string, message PublishRequest) {
	payload, err := json.Marshal(message)
	if err != nil {
		return
	}
	reqCtx, cancel := context.WithTimeout(context.Background(), callbackTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, strings.NewReader(string(payload)))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf("Error sending message to endpoint %s: %v\n", url, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Printf("Callback endpoint %s returned HTTP %d\n", url, resp.StatusCode)
	}
}

func subscriptionFromKeyParts(key, callbackURL string) (subscriptionInfo, error) {
	parts := strings.SplitN(key, ":", 4)
	if len(parts) != 4 || parts[0] != "as" {
		return subscriptionInfo{}, fmt.Errorf("unexpected active subscription key %q", key)
	}
	return subscriptionInfo{
		TenantID:    parts[1],
		ID:          parts[2],
		Key:         key,
		Channel:     parts[3],
		CallbackURL: callbackURL,
	}, nil
}

func subscriptionFromKey(ctx context.Context, redisClient *redis.Client, key string) (subscriptionInfo, error) {
	callbackURL, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		return subscriptionInfo{}, err
	}
	return subscriptionFromKeyParts(key, callbackURL)
}

func scanActiveSubscriptions(ctx context.Context, scope tenantScope, pattern string) ([]subscriptionInfo, error) {
	subscriptions := make([]subscriptionInfo, 0)
	collect := callbacks.Callback(func(ctx context.Context, redisClient *redis.Client, key string) ([]byte, error) {
		subscription, err := subscriptionFromKey(ctx, redisClient, key)
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil, nil
			}
			return nil, err
		}
		subscriptions = append(subscriptions, subscription)
		return nil, nil
	})
	_, _, err := scanner.RunScan(ctx, pattern, 0, scope.Client, nil, nil, []callbacks.Callback{collect})
	if err != nil {
		return nil, err
	}
	sort.Slice(subscriptions, func(i, j int) bool { return subscriptions[i].ID < subscriptions[j].ID })
	return subscriptions, nil
}

func scanGroupMembers(ctx context.Context, scope tenantScope, groupID string) ([]subscriptionInfo, error) {
	members := make([]subscriptionInfo, 0)
	prefix := subscriptionGroupPattern(scope.ID, groupID, "", "")
	collect := callbacks.Callback(func(ctx context.Context, redisClient *redis.Client, key string) ([]byte, error) {
		callbackURL, err := redisClient.Get(ctx, key).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil, nil
			}
			return nil, err
		}
		parts := strings.SplitN(key, ":", 5)
		if len(parts) != 5 || parts[0] != "sg" || parts[1] != scope.ID || parts[2] != groupID {
			return nil, fmt.Errorf("unexpected subscription group key %q", key)
		}
		members = append(members, subscriptionInfo{
			TenantID:    scope.ID,
			ID:          parts[3],
			Channel:     parts[4],
			CallbackURL: callbackURL,
		})
		return nil, nil
	})
	_, _, err := scanner.RunScan(ctx, prefix+"*", 0, scope.Client, nil, nil, []callbacks.Callback{collect})
	return members, err
}

func activeChannelsHandler(w http.ResponseWriter, r *http.Request) {
	scope, ok := requestTenantScope(w, r)
	if !ok {
		return
	}
	channels, err := scope.Client.PubSubChannels(r.Context(), "").Result()
	if err != nil {
		http.Error(w, "Error retrieving Redis channels", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, channels)
}

func listActiveSubscriptions(w http.ResponseWriter, r *http.Request) {
	scope, ok := requestTenantScope(w, r)
	if !ok {
		return
	}
	subscriptions, err := scanActiveSubscriptions(r.Context(), scope, activeSubscriptionPattern(scope.ID, "*", "*"))
	if err != nil {
		http.Error(w, "Error retrieving active subscriptions", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, subscriptions)
}

func deleteActiveSubscription(w http.ResponseWriter, r *http.Request) {
	scope, ok := requestTenantScope(w, r)
	if !ok {
		return
	}
	id := chi.URLParam(r, "subscriptionID")
	if id == "" {
		http.Error(w, "subscription ID is required", http.StatusBadRequest)
		return
	}
	subscriptions, err := scanActiveSubscriptions(r.Context(), scope, activeSubscriptionPattern(scope.ID, id, "*"))
	if err != nil {
		http.Error(w, "Error resolving active subscription", http.StatusInternalServerError)
		return
	}
	if len(subscriptions) == 0 {
		http.Error(w, "Active subscription not found", http.StatusNotFound)
		return
	}
	deleted := 0
	for _, subscription := range subscriptions {
		if manager.cancelSubscription(subscription.Key) {
			deleted++
		}
	}
	if deleted == 0 {
		http.Error(w, "Active subscription is no longer running", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func subscribeHandler(w http.ResponseWriter, r *http.Request) {
	scope, ok := requestTenantScope(w, r)
	if !ok {
		return
	}
	channelName := chi.URLParam(r, "channelName")
	var body struct {
		CallbackURL string `json:"callbackURL"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}
	if body.CallbackURL == "" {
		body.CallbackURL = os.Getenv("PUBSUB_DEFAULT_CALLBACK_URL")
	}
	if body.CallbackURL == "" {
		http.Error(w, "Default callback URL not set", http.StatusInternalServerError)
		return
	}
	info, err := startSubscription(scope, channelName, body.CallbackURL)
	if err != nil {
		http.Error(w, "Error starting Redis subscription", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, info)
}

func generateGroupID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func parseBoolQuery(r *http.Request, name string) (bool, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return false, nil
	}
	switch strings.ToLower(value) {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be true or false", name)
	}
}

func saveSubscriptionGroup(w http.ResponseWriter, r *http.Request) {
	scope, ok := requestTenantScope(w, r)
	if !ok {
		return
	}
	all, err := parseBoolQuery(r, "all")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	shutdown, err := parseBoolQuery(r, "shutdown")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	active, err := scanActiveSubscriptions(r.Context(), scope, activeSubscriptionPattern(scope.ID, "*", "*"))
	if err != nil {
		http.Error(w, "Error retrieving active subscriptions", http.StatusInternalServerError)
		return
	}

	selected := active
	if !all {
		var body saveGroupRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "subscriptionIDs body is required when all is false", http.StatusBadRequest)
			return
		}
		if len(body.SubscriptionIDs) == 0 {
			http.Error(w, "subscriptionIDs must not be empty", http.StatusBadRequest)
			return
		}
		wanted := make(map[string]struct{}, len(body.SubscriptionIDs))
		for _, id := range body.SubscriptionIDs {
			if id != "" {
				wanted[id] = struct{}{}
			}
		}
		selected = selected[:0]
		for _, subscription := range active {
			if _, exists := wanted[subscription.ID]; exists {
				selected = append(selected, subscription)
				delete(wanted, subscription.ID)
			}
		}
		if len(wanted) != 0 {
			missing := make([]string, 0, len(wanted))
			for id := range wanted {
				missing = append(missing, id)
			}
			sort.Strings(missing)
			http.Error(w, "active subscription IDs not found: "+strings.Join(missing, ", "), http.StatusNotFound)
			return
		}
	}

	groupID := generateGroupID()
	pipe := scope.Client.Pipeline()
	for _, subscription := range selected {
		key := subscriptionGroupPattern(scope.ID, groupID, subscription.ID, subscription.Channel)
		pipe.Set(r.Context(), key, subscription.CallbackURL, 0)
	}
	if _, err := pipe.Exec(r.Context()); err != nil {
		http.Error(w, "Error saving subscription group", http.StatusInternalServerError)
		return
	}

	shutdownCount := 0
	if shutdown {
		for _, subscription := range selected {
			if manager.cancelSubscription(subscription.Key) {
				shutdownCount++
			}
		}
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"tenantID":      scope.ID,
		"groupID":       groupID,
		"subscriptions": len(selected),
		"shutdown":      shutdownCount,
	})
}

func loadSubscriptionGroup(w http.ResponseWriter, r *http.Request) {
	scope, ok := requestTenantScope(w, r)
	if !ok {
		return
	}
	groupID := chi.URLParam(r, "groupID")
	members, err := scanGroupMembers(r.Context(), scope, groupID)
	if err != nil {
		http.Error(w, "Error retrieving subscription group data", http.StatusInternalServerError)
		return
	}
	if len(members) == 0 {
		http.Error(w, "Subscription group not found", http.StatusNotFound)
		return
	}
	loaded := 0
	for _, member := range members {
		if _, err := startSubscription(scope, member.Channel, member.CallbackURL); err == nil {
			loaded++
		}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"tenantID":      scope.ID,
		"groupID":       groupID,
		"subscriptions": loaded,
	})
}

func listSubscriptionGroups(w http.ResponseWriter, r *http.Request) {
	scope, ok := requestTenantScope(w, r)
	if !ok {
		return
	}
	groupSet := make(map[string]struct{})
	collect := callbacks.Callback(func(_ context.Context, _ *redis.Client, key string) ([]byte, error) {
		parts := strings.SplitN(key, ":", 4)
		if len(parts) >= 3 && parts[0] == "sg" && parts[1] == scope.ID && parts[2] != "" {
			groupSet[parts[2]] = struct{}{}
		}
		return nil, nil
	})
	_, _, err := scanner.RunScan(r.Context(), subscriptionGroupPattern(scope.ID, "*", "*", "*"), 0, scope.Client, nil, nil, []callbacks.Callback{collect})
	if err != nil {
		http.Error(w, "Error retrieving subscription groups", http.StatusInternalServerError)
		return
	}
	groupIDs := make([]string, 0, len(groupSet))
	for id := range groupSet {
		groupIDs = append(groupIDs, id)
	}
	sort.Strings(groupIDs)
	writeJSON(w, http.StatusOK, groupIDs)
}

func bootstrapHandler(w http.ResponseWriter, r *http.Request) {
	scope, err := defaultTenantScope()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defaultCallbackURL := os.Getenv("PUBSUB_DEFAULT_CALLBACK_URL")
	axiomURL := os.Getenv("DEFAULT_AXIOM_URL")
	requests := []struct {
		channel     string
		callbackURL string
	}{
		{"dev:global:logs:rate_limiters", defaultCallbackURL},
		{"dev:global:logs:rate_limiters", axiomURL},
		{"dev:global:logs:1", defaultCallbackURL},
	}
	for _, request := range requests {
		if request.callbackURL == "" {
			http.Error(w, "Bootstrap callback URL is not configured", http.StatusInternalServerError)
			return
		}
		if _, err := startSubscription(scope, request.channel, request.callbackURL); err != nil {
			http.Error(w, "Bootstrap subscription failed", http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Bootstrap successful\n"))
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		fmt.Printf("Error encoding JSON response: %v\n", err)
	}
}

func NewRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(authenticateAPIKey)

	r.Route("/channels", func(r chi.Router) {
		r.Get("/", activeChannelsHandler)
		r.Post("/{channelName}/subscribe", subscribeHandler)
	})

	r.Route("/subscriptions", func(r chi.Router) {
		r.Get("/", listActiveSubscriptions)
		r.Delete("/{subscriptionID}", deleteActiveSubscription)
	})

	r.Route("/groups", func(r chi.Router) {
		r.Get("/", listSubscriptionGroups)
		r.Post("/", saveSubscriptionGroup)
		r.Post("/{groupID}/load", loadSubscriptionGroup)
	})

	r.Post("/functions", registerFunctionHandler)
	r.Get("/bootstrap", bootstrapHandler)
	return r
}

func ShutdownSubscriptions(ctx context.Context) error {
	return manager.shutdownSubscriptions(ctx)
}

func Shutdown(ctx context.Context) error {
	if err := manager.shutdownSubscriptions(ctx); err != nil {
		return err
	}
	return tenantClients.close()
}
