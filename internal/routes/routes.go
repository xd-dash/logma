package routes

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
)

var (
	client = redis.NewClient(redisOptionsFromEnv())

	rootCtx, rootCancel = context.WithCancel(context.Background())

	manager = newSubscriptionManager()

	httpClient = &http.Client{
		Timeout: 30 * time.Second,
	}
)

const (
	apiKeyHeader               = "X-API-Key"
	publicAPIPrefix            = "/pubsub/api/v0.0.1"
	activeSubscriptionsPattern = "active_subscriptions:%s:%s"
	subscriptionGroupsPrefix   = "subscription_groups"

	redisScanCount = 1000

	callbackQueueSize   = 256
	callbackWorkerCount = 4
	callbackTimeout     = 30 * time.Second
)

var (
	errSubscriptionManagerStopped = errors.New("subscription manager is stopped")
)

type SubscriptionListener struct {
	Channel        string
	Callbacks      callbackConfig
	Tenant         string
	RedisClient    *redis.Client
	OwnRedisClient bool
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
	ID        string
	Key       string
	Channel   string
	Callbacks callbackConfig
}

type subscribeResponse struct {
	SubscriptionID string           `json:"subscriptionID"`
	Channel        string           `json:"channel"`
	Callbacks      []callbackScheme `json:"callbacks"`
}

type subscriptionGroupMember struct {
	SubscriptionID string           `json:"subscriptionID"`
	Channel        string           `json:"channel"`
	Callbacks      []callbackScheme `json:"callbacks"`
}

type subscriptionGroupResponse struct {
	GroupID string                    `json:"groupID"`
	Members []subscriptionGroupMember `json:"members"`
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
	registerCh   chan registerSubscription
	unregisterCh chan string
	cancelCh     chan cancelSubscription
	shutdownCh   chan chan struct{}
	done         chan struct{}
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
	d := &callbackDispatcher{
		jobs: make(chan callbackJob, callbackQueueSize),
	}

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

func (d *callbackDispatcher) dispatch(
	callback func(url string, message PublishRequest),
	url string,
	message PublishRequest,
) {
	job := callbackJob{
		callback: callback,
		url:      url,
		message:  message,
	}

	select {
	case d.jobs <- job:
	default:
		fmt.Printf(
			"Callback queue full; dropping callback for %s\n",
			url,
		)
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
		shutdownCh:   make(chan chan struct{}),
		done:         make(chan struct{}),
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
		case registration := <-m.registerCh:
			if shuttingDown {
				registration.response <- errSubscriptionManagerStopped
				continue
			}

			subscriptions[registration.key] = registration.cancel
			active++

			registration.response <- nil

		case key := <-m.unregisterCh:
			if _, exists := subscriptions[key]; !exists {
				continue
			}

			delete(subscriptions, key)
			active--

			if shuttingDown && active == 0 {
				if shutdownComplete != nil {
					close(shutdownComplete)
					shutdownComplete = nil
				}

				return
			}

		case request := <-m.cancelCh:
			cancel, exists := subscriptions[request.key]

			if !exists {
				request.response <- false
				continue
			}

			cancel()
			request.response <- true

		case complete := <-m.shutdownCh:
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
				shutdownComplete = nil
				return
			}
		}
	}
}

func (m *subscriptionManager) register(
	key string,
	cancel context.CancelFunc,
) error {
	response := make(chan error, 1)

	request := registerSubscription{
		key:      key,
		cancel:   cancel,
		response: response,
	}

	select {
	case <-m.done:
		return errSubscriptionManagerStopped

	case m.registerCh <- request:
		return <-response
	}
}

func (m *subscriptionManager) unregister(key string) {
	select {
	case <-m.done:
		return

	case m.unregisterCh <- key:
	}
}

func (m *subscriptionManager) cancelSubscription(
	key string,
) bool {
	response := make(chan bool, 1)

	request := cancelSubscription{
		key:      key,
		response: response,
	}

	select {
	case <-m.done:
		return false

	case m.cancelCh <- request:
		return <-response
	}
}

func (m *subscriptionManager) shutdown(
	ctx context.Context,
) error {
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
			http.Error(
				w,
				"API key is not set",
				http.StatusInternalServerError,
			)
			return
		}

		requestAPIKey := r.Header.Get(apiKeyHeader)
		if requestAPIKey != apiKey {
			http.Error(
				w,
				"Invalid API key",
				http.StatusUnauthorized,
			)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func startSubscription(
	channelName string,
	callbackURL string,
) (*subscriptionInfo, error) {
	return startSubscriptionWithCallbacks(
		channelName,
		singleHTTPCallbackConfig(callbackURL),
	)
}

func startSubscriptionWithCallbacks(
	channelName string,
	callbacks callbackConfig,
) (*subscriptionInfo, error) {
	return startSubscriptionForPrincipal(
		channelName,
		callbacks,
		requestPrincipal{},
	)
}

func startSubscriptionForPrincipal(
	channelName string,
	callbacks callbackConfig,
	principal requestPrincipal,
) (*subscriptionInfo, error) {
	if channelName == "" {
		return nil, errors.New("channel name is empty")
	}

	callbacks, err := normalizeCallbackConfig(callbacks)
	if err != nil {
		return nil, err
	}
	if callbackConfigEmpty(callbacks) {
		return nil, errors.New("callback configuration is empty")
	}

	subCtx, cancel := context.WithCancel(rootCtx)

	if err := subCtx.Err(); err != nil {
		cancel()
		return nil, errSubscriptionManagerStopped
	}

	subscriptionClient := client
	ownSubscriptionClient := false
	if principal.Tenant != "" {
		subscriptionClient = redis.NewClient(
			redisOptionsForCredentials(
				principal.Username,
				principal.Password,
			),
		)
		ownSubscriptionClient = true
	}

	clientHandedOff := false
	defer func() {
		if ownSubscriptionClient && !clientHandedOff {
			_ = subscriptionClient.Close()
		}
	}()

	pubsub := subscriptionClient.Subscribe(
		subCtx,
		channelName,
	)

	if _, err := pubsub.Receive(subCtx); err != nil {
		cancel()
		_ = pubsub.Close()

		return nil, fmt.Errorf(
			"subscribe to Redis channel %q: %w",
			channelName,
			err,
		)
	}

	subscriptionKey := generateTempChannelID(channelName)
	subscriptionID := extractSubscriptionID(subscriptionKey)

	storedCallbacks, err := encodeStoredCallbackConfig(callbacks)
	if err != nil {
		cancel()
		_ = pubsub.Close()
		return nil, fmt.Errorf(
			"encode callback configuration for %q: %w",
			channelName,
			err,
		)
	}

	if err := client.Set(
		subCtx,
		subscriptionKey,
		storedCallbacks,
		0,
	).Err(); err != nil {
		cancel()

		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_ = pubsub.Unsubscribe(
			cleanupCtx,
			channelName,
		)
		_ = pubsub.Close()

		return nil, fmt.Errorf(
			"save subscription state for %q: %w",
			channelName,
			err,
		)
	}

	if err := manager.register(
		subscriptionKey,
		cancel,
	); err != nil {
		cancel()

		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_ = pubsub.Unsubscribe(
			cleanupCtx,
			channelName,
		)
		_ = client.Del(
			cleanupCtx,
			subscriptionKey,
		)
		_ = pubsub.Close()

		return nil, err
	}

	info := &subscriptionInfo{
		ID:        subscriptionID,
		Key:       subscriptionKey,
		Channel:   channelName,
		Callbacks: callbacks,
	}

	clientHandedOff = true
	go runSubscription(
		subCtx,
		pubsub,
		SubscriptionListener{
			Channel:        channelName,
			Callbacks:      callbacks,
			Tenant:         principal.Tenant,
			RedisClient:    subscriptionClient,
			OwnRedisClient: ownSubscriptionClient,
		},
		info,
	)

	return info, nil
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
	if listener.OwnRedisClient && listener.RedisClient != nil {
		defer listener.RedisClient.Close()
	}

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := pubsub.Unsubscribe(
			cleanupCtx,
			listener.Channel,
		); err != nil {
			fmt.Printf(
				"Error unsubscribing from channel %s: %v\n",
				listener.Channel,
				err,
			)
		}

		if err := client.Del(
			cleanupCtx,
			info.Key,
		).Err(); err != nil {
			fmt.Printf(
				"Error removing subscription data from Redis key %s: %v\n",
				info.Key,
				err,
			)
		}

		if err := pubsub.Close(); err != nil {
			fmt.Printf(
				"Error closing Redis Pub/Sub for channel %s: %v\n",
				listener.Channel,
				err,
			)
		}
	}()

	pubsubChannel := pubsub.Channel()

	for {
		select {
		case <-subCtx.Done():
			return

		case msg, ok := <-pubsubChannel:
			if !ok {
				return
			}

			if msg.Payload == "" || msg.Payload == "{}" {
				fmt.Println("Empty message received")
				continue
			}

			var message PublishRequest

			if err := json.Unmarshal(
				[]byte(msg.Payload),
				&message,
			); err != nil {
				fmt.Printf(
					"Error decoding message: %v\n",
					err,
				)
				continue
			}

			dispatchCallbackConfig(
				dispatcher,
				listener.Callbacks,
				message,
				listener.Tenant,
				listener.RedisClient,
			)

			if message.Type == "Signal" &&
				message.Message == "UNSUBSCRIBE" {
				return
			}
		}
	}
}

func sendMessageToEndpoint(
	url string,
	message PublishRequest,
) {
	payload, err := json.Marshal(message)
	if err != nil {
		fmt.Printf(
			"Error marshaling message: %v\n",
			err,
		)
		return
	}

	reqCtx, cancel := context.WithTimeout(
		context.Background(),
		callbackTimeout,
	)
	defer cancel()

	req, err := http.NewRequestWithContext(
		reqCtx,
		http.MethodPost,
		url,
		strings.NewReader(string(payload)),
	)
	if err != nil {
		fmt.Printf(
			"Error creating callback request to %s: %v\n",
			url,
			err,
		)
		return
	}

	req.Header.Set(
		"Content-Type",
		"application/json",
	)

	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf(
			"Error sending message to endpoint %s: %v\n",
			url,
			err,
		)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Printf(
			"Callback endpoint %s returned HTTP %d\n",
			url,
			resp.StatusCode,
		)
	}
}

func generateTempChannelID(channelName string) string {
	return fmt.Sprintf(
		"active_subscriptions:%d:%s",
		time.Now().UnixNano(),
		channelName,
	)
}

func extractSubscriptionID(key string) string {
	parts := strings.SplitN(
		key,
		":",
		3,
	)

	if len(parts) != 3 {
		return ""
	}

	return parts[1]
}

func activeChannelsHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	channels, err := client.PubSubChannels(
		rootCtx,
		"",
	).Result()
	if err != nil {
		http.Error(
			w,
			"Error retrieving Redis channels",
			http.StatusInternalServerError,
		)
		return
	}

	if principal, ok := principalFromRequest(r); ok &&
		principal.Tenant != "" {
		prefix := "tenant:" + principal.Tenant + ":"
		filtered := channels[:0]
		for _, channel := range channels {
			if strings.HasPrefix(channel, prefix) {
				filtered = append(filtered, channel)
			}
		}
		channels = filtered
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	if err := json.NewEncoder(w).Encode(
		channels,
	); err != nil {
		fmt.Printf(
			"Error encoding Redis channels: %v\n",
			err,
		)
	}
}

func bootstrapHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	defaultCallbackURL := os.Getenv(
		"PUBSUB_DEFAULT_CALLBACK_URL",
	)

	axiomURL := os.Getenv(
		"DEFAULT_AXIOM_URL",
	)

	requests := []struct {
		channel     string
		callbackURL string
	}{
		{
			channel:     "dev:global:logs:rate_limiters",
			callbackURL: defaultCallbackURL,
		},
		{
			channel:     "dev:global:logs:rate_limiters",
			callbackURL: axiomURL,
		},
		{
			channel:     "dev:global:logs:1",
			callbackURL: defaultCallbackURL,
		},
	}

	for _, request := range requests {
		if request.callbackURL == "" {
			http.Error(
				w,
				"Bootstrap callback URL is not configured",
				http.StatusInternalServerError,
			)
			return
		}

		if _, err := startSubscription(
			request.channel,
			request.callbackURL,
		); err != nil {
			http.Error(
				w,
				fmt.Sprintf(
					"Bootstrap subscription failed: %v",
					err,
				),
				http.StatusInternalServerError,
			)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(
		[]byte("Bootstrap successful\n"),
	)
}

func NewRouter() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(authenticateRequest)

	// Legacy API surface. Keep this available while callers move to the
	// versioned /pubsub API.
	r.Route("/channels", func(r chi.Router) {
		r.Post("/{channelName}/subscribe", subscribeHandler)
		r.Get("/", activeChannelsHandler)
		r.Post("/groups/save", saveSubscriptionGroup)
		r.Get("/groups/load/{groupID}", loadSubscriptionGroup)
		r.Get("/groups", listSubscriptionGroups)
	})

	r.Get("/bootstrap", bootstrapHandler)

	// Canonical public Pub/Sub API. Channels remain runtime callback
	// subscriptions; subscription groups are saved channel/callback sets.
	r.Get(publicAPIPrefix+"/auth/profile", authProfileHandler)

	r.Route(publicAPIPrefix+"/users", func(r chi.Router) {
		r.Use(requireApplicationAdmin)
		r.Get("/", listACLUsersHandler)
		r.Post("/", createACLUserHandler)
		r.Put("/{username}", replaceACLUserHandler)
		r.Delete("/{username}", deleteACLUserHandler)
	})

	r.Route(publicAPIPrefix+"/functions", func(r chi.Router) {
		r.Get("/", listTenantFunctionsHandler)
		r.Post("/", uploadTenantFunctionHandler)
		r.Delete("/{name}", deleteTenantFunctionHandler)
	})

	r.Route(publicAPIPrefix+"/channels", func(r chi.Router) {
		r.Get("/", activeChannelsHandler)
		r.Post("/", createChannelHandler)

		r.Post("/save", managedGroupGuard(saveActiveChannelsGroup))
		r.Get("/groups", managedGroupGuard(listSubscriptionGroups))
		r.Post("/groups", managedGroupGuard(createSubscriptionGroup))
		r.Get("/groups/{groupID}", managedGroupGuard(getSubscriptionGroup))
		r.Delete("/groups/{groupID}", managedGroupGuard(deleteSubscriptionGroup))
		r.Post("/groups/{groupID}/load", managedGroupGuard(loadSubscriptionGroup))

		r.Post("/{channelName}", createChannelHandler)
		r.Post("/{channelName}/publish", publishChannelHandler)
		r.Get("/{channelName}/subscribe", subscribeHandler)
		r.Post("/{channelName}/subscribe", subscribeHandler)
	})

	return r
}

func createChannelHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	channelName := strings.TrimSpace(
		chi.URLParam(r, "channelName"),
	)
	if channelName == "" {
		channelName = strings.TrimSpace(
			r.URL.Query().Get("name"),
		)
	}
	if channelName == "" {
		var err error
		channelName, err = generateChannelName()
		if err != nil {
			http.Error(
				w,
				"Error generating channel name",
				http.StatusInternalServerError,
			)
			return
		}
	}

	startChannelCallback(
		w,
		r,
		channelName,
	)
}

func subscribeHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	channelName := strings.TrimSpace(
		chi.URLParam(r, "channelName"),
	)
	if channelName == "" {
		http.Error(
			w,
			"Channel name is required",
			http.StatusBadRequest,
		)
		return
	}

	startChannelCallback(
		w,
		r,
		channelName,
	)
}

func startChannelCallback(
	w http.ResponseWriter,
	r *http.Request,
	channelName string,
) {
	callbacks, err := parseCallbackConfigFromRequest(r)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	if callbackConfigEmpty(callbacks) {
		defaultCallbackURL := strings.TrimSpace(
			os.Getenv("PUBSUB_DEFAULT_CALLBACK_URL"),
		)
		if defaultCallbackURL != "" {
			callbacks = singleHTTPCallbackConfig(
				defaultCallbackURL,
			)
		}
	}
	if callbackConfigEmpty(callbacks) {
		http.Error(
			w,
			"Callback configuration is required",
			http.StatusBadRequest,
		)
		return
	}

	principal, _ := principalFromRequest(r)
	if authProfileFromEnv().Name == "managed" && principal.Tenant != "" {
		channelName, err = scopeChannelForPrincipal(principal, channelName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		for _, callback := range callbacks.Callbacks {
			if err := validateTenantFunctionCallback(principal, callback); err != nil {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
		}
	}

	info, err := startSubscriptionForPrincipal(
		channelName,
		callbacks,
		principal,
	)
	if err != nil {
		fmt.Printf(
			"Error starting subscription for channel %s: %v\n",
			channelName,
			err,
		)

		http.Error(
			w,
			"Error starting Redis subscription",
			http.StatusInternalServerError,
		)
		return
	}

	response := subscribeResponse{
		SubscriptionID: info.ID,
		Channel:        info.Channel,
		Callbacks:      info.Callbacks.Callbacks,
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)
	if r.Method == http.MethodGet {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusCreated)
	}

	if err := json.NewEncoder(
		w,
	).Encode(response); err != nil {
		fmt.Printf(
			"Error encoding subscription response: %v\n",
			err,
		)
	}
}

func generateChannelName() (string, error) {
	var random [8]byte

	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}

	return "channel-" + hex.EncodeToString(
		random[:],
	), nil
}

func generateGroupID() string {
	return fmt.Sprintf(
		"%d",
		time.Now().UnixNano(),
	)
}

func scanKeys(pattern string) ([]string, error) {
	keys := make(
		[]string,
		0,
	)

	iter := client.Scan(
		rootCtx,
		0,
		pattern,
		redisScanCount,
	).Iterator()

	for iter.Next(rootCtx) {
		keys = append(
			keys,
			iter.Val(),
		)
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return keys, nil
}

func saveSubscriptionGroup(
	w http.ResponseWriter,
	r *http.Request,
) {
	activeSubscriptions, err := scanKeys(
		fmt.Sprintf(
			activeSubscriptionsPattern,
			"*",
			"*",
		),
	)
	if err != nil {
		http.Error(
			w,
			"Error retrieving active subscriptions data",
			http.StatusInternalServerError,
		)
		return
	}

	groupID := generateGroupID()

	for _, key := range activeSubscriptions {
		storedCallbacks, err := client.Get(
			rootCtx,
			key,
		).Result()
		if err != nil {
			if errors.Is(
				err,
				redis.Nil,
			) {
				continue
			}

			fmt.Printf(
				"Error retrieving callback URL for key %s: %v\n",
				key,
				err,
			)
			continue
		}

		parts := strings.SplitN(
			key,
			":",
			3,
		)

		if len(parts) != 3 {
			fmt.Printf(
				"Unexpected key format: %s\n",
				key,
			)
			continue
		}

		activeSubscriptionID := parts[1]
		channelName := parts[2]

		subscriptionKey := fmt.Sprintf(
			"subscription_groups:%s:%s:%s",
			groupID,
			activeSubscriptionID,
			channelName,
		)

		if err := client.Set(
			rootCtx,
			subscriptionKey,
			storedCallbacks,
			0,
		).Err(); err != nil {
			http.Error(
				w,
				fmt.Sprintf(
					"Error saving subscription group with key %s: %v",
					subscriptionKey,
					err,
				),
				http.StatusInternalServerError,
			)
			return
		}
	}

	w.Header().Set(
		"Content-Type",
		"text/plain",
	)

	w.WriteHeader(http.StatusOK)

	_, _ = w.Write(
		[]byte(groupID),
	)
}

func loadSubscriptionGroup(
	w http.ResponseWriter,
	r *http.Request,
) {
	groupID := chi.URLParam(
		r,
		"groupID",
	)

	groupMembers, err := scanKeys(
		fmt.Sprintf(
			"%s:%s:*",
			subscriptionGroupsPrefix,
			groupID,
		),
	)
	if err != nil {
		http.Error(
			w,
			"Error retrieving subscription group data",
			http.StatusInternalServerError,
		)
		return
	}

	loaded := 0

	for _, key := range groupMembers {
		storedCallbacks, err := client.Get(
			rootCtx,
			key,
		).Result()
		if err != nil {
			if errors.Is(
				err,
				redis.Nil,
			) {
				continue
			}

			fmt.Printf(
				"Error retrieving callback URL for key %s: %v\n",
				key,
				err,
			)
			continue
		}

		channelName, ok := extractGroupChannelName(
			key,
			groupID,
		)
		if !ok {
			fmt.Printf(
				"Unexpected group key format: %s\n",
				key,
			)
			continue
		}

		callbacks, err := decodeStoredCallbackConfig(
			storedCallbacks,
		)
		if err != nil {
			fmt.Printf(
				"Error decoding callbacks for channel %s: %v\n",
				channelName,
				err,
			)
			continue
		}

		if _, err := startSubscriptionWithCallbacks(
			channelName,
			callbacks,
		); err != nil {
			fmt.Printf(
				"Error loading subscription for channel %s: %v\n",
				channelName,
				err,
			)
			continue
		}

		loaded++
	}

	response := map[string]interface{}{
		"groupID":       groupID,
		"subscriptions": loaded,
		"message":       "Subscription group loaded",
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(
		w,
	).Encode(response); err != nil {
		fmt.Printf(
			"Error encoding group load response: %v\n",
			err,
		)
	}
}

func extractGroupChannelName(
	key string,
	groupID string,
) (string, bool) {
	prefix := fmt.Sprintf(
		"%s:%s:",
		subscriptionGroupsPrefix,
		groupID,
	)

	if !strings.HasPrefix(
		key,
		prefix,
	) {
		return "", false
	}

	remainder := strings.TrimPrefix(
		key,
		prefix,
	)

	parts := strings.SplitN(
		remainder,
		":",
		2,
	)

	if len(parts) != 2 {
		return "", false
	}

	return parts[1], true
}

func getSubscriptionGroup(
	w http.ResponseWriter,
	r *http.Request,
) {
	groupID := strings.TrimSpace(
		chi.URLParam(r, "groupID"),
	)
	members, err := readSubscriptionGroup(groupID)
	if err != nil {
		http.Error(
			w,
			"Error retrieving subscription group data",
			http.StatusInternalServerError,
		)
		return
	}
	if len(members) == 0 {
		http.Error(
			w,
			"Subscription group not found",
			http.StatusNotFound,
		)
		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(
		subscriptionGroupResponse{
			GroupID: groupID,
			Members: members,
		},
	); err != nil {
		fmt.Printf(
			"Error encoding subscription group: %v\n",
			err,
		)
	}
}

func deleteSubscriptionGroup(
	w http.ResponseWriter,
	r *http.Request,
) {
	groupID := strings.TrimSpace(
		chi.URLParam(r, "groupID"),
	)
	keys, err := scanKeys(
		fmt.Sprintf(
			"%s:%s:*",
			subscriptionGroupsPrefix,
			groupID,
		),
	)
	if err != nil {
		http.Error(
			w,
			"Error retrieving subscription group data",
			http.StatusInternalServerError,
		)
		return
	}
	if len(keys) == 0 {
		http.Error(
			w,
			"Subscription group not found",
			http.StatusNotFound,
		)
		return
	}

	deleted, err := client.Del(
		rootCtx,
		keys...,
	).Result()
	if err != nil {
		http.Error(
			w,
			"Error deleting subscription group",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(
		map[string]interface{}{
			"groupID": groupID,
			"deleted": deleted,
		},
	)
}

func readSubscriptionGroup(
	groupID string,
) ([]subscriptionGroupMember, error) {
	keys, err := scanKeys(
		fmt.Sprintf(
			"%s:%s:*",
			subscriptionGroupsPrefix,
			groupID,
		),
	)
	if err != nil {
		return nil, err
	}

	members := make(
		[]subscriptionGroupMember,
		0,
		len(keys),
	)

	for _, key := range keys {
		storedCallbacks, err := client.Get(
			rootCtx,
			key,
		).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return nil, err
		}

		prefix := fmt.Sprintf(
			"%s:%s:",
			subscriptionGroupsPrefix,
			groupID,
		)
		remainder := strings.TrimPrefix(
			key,
			prefix,
		)
		parts := strings.SplitN(
			remainder,
			":",
			2,
		)
		if len(parts) != 2 ||
			parts[0] == "" ||
			parts[1] == "" {
			continue
		}

		callbacks, err := decodeStoredCallbackConfig(
			storedCallbacks,
		)
		if err != nil {
			return nil, err
		}

		members = append(
			members,
			subscriptionGroupMember{
				SubscriptionID: parts[0],
				Channel:        parts[1],
				Callbacks:      callbacks.Callbacks,
			},
		)
	}

	sort.Slice(
		members,
		func(i, j int) bool {
			if members[i].Channel == members[j].Channel {
				return members[i].SubscriptionID <
					members[j].SubscriptionID
			}

			return members[i].Channel <
				members[j].Channel
		},
	)

	return members, nil
}

func listSubscriptionGroups(
	w http.ResponseWriter,
	r *http.Request,
) {
	groupKeys, err := scanKeys(
		fmt.Sprintf(
			"%s:*",
			subscriptionGroupsPrefix,
		),
	)
	if err != nil {
		http.Error(
			w,
			"Error retrieving subscription group keys",
			http.StatusInternalServerError,
		)
		return
	}

	groupSet := make(
		map[string]struct{},
	)

	prefix := subscriptionGroupsPrefix + ":"

	for _, key := range groupKeys {
		remainder := strings.TrimPrefix(
			key,
			prefix,
		)

		parts := strings.SplitN(
			remainder,
			":",
			2,
		)

		if len(parts) == 0 || parts[0] == "" {
			continue
		}

		groupSet[parts[0]] = struct{}{}
	}

	groupIDs := make(
		[]string,
		0,
		len(groupSet),
	)

	for groupID := range groupSet {
		groupIDs = append(
			groupIDs,
			groupID,
		)
	}

	sort.Strings(groupIDs)

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(
		w,
	).Encode(groupIDs); err != nil {
		fmt.Printf(
			"Error encoding subscription group IDs: %v\n",
			err,
		)
	}
}

func stopSubscription(
	subscriptionKey string,
) bool {
	return manager.cancelSubscription(
		subscriptionKey,
	)
}

func ShutdownSubscriptions(
	ctx context.Context,
) error {
	return manager.shutdown(ctx)
}

func Shutdown(
	ctx context.Context,
) error {
	if err := manager.shutdown(ctx); err != nil {
		return err
	}

	return client.Close()
}
