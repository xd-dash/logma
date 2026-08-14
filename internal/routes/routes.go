package routes

import (
	"context"
	"encoding/json"
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
	client = redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_URI"),
		Password: os.Getenv("REDISCLI_AUTH"),
		DB:       0,
	})

	rootCtx, rootCancel = context.WithCancel(context.Background())

	wg sync.WaitGroup

	subscriptionsMu sync.Mutex
	subscriptions   = make(map[string]context.CancelFunc)

	httpClient = &http.Client{
		Timeout: 30 * time.Second,
	}
)

const (
	apiKeyHeader               = "X-API-Key"
	activeSubscriptionsPattern = "active_subscriptions:%s:%s"
	subscriptionGroupsPrefix   = "subscription_groups"

	redisScanCount = 1000
)

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
	ID          string
	Key         string
	Channel     string
	CallbackURL string
	Cancel      context.CancelFunc
}

type subscribeResponse struct {
	SubscriptionID string `json:"subscriptionID"`
	Channel        string `json:"channel"`
	CallbackURL    string `json:"callbackURL"`
}

func authenticateAPIKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := os.Getenv("API_KEY")
		if apiKey == "" {
			http.Error(w, "API key is not set", http.StatusInternalServerError)
			return
		}

		requestAPIKey := r.Header.Get(apiKeyHeader)
		if requestAPIKey != apiKey {
			http.Error(w, "Invalid API key", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func startSubscription(
	channelName string,
	callbackURL string,
) (*subscriptionInfo, error) {
	subscriptionKey := generateTempChannelID(channelName)
	subscriptionID := extractSubscriptionID(subscriptionKey)

	subCtx, cancel := context.WithCancel(rootCtx)

	pubsub := client.Subscribe(subCtx, channelName)

	// Wait for Redis to confirm the subscription rather than using
	// an unrelated PING on the regular Redis client.
	if _, err := pubsub.Receive(subCtx); err != nil {
		cancel()
		_ = pubsub.Close()
		return nil, fmt.Errorf(
			"subscribe to Redis channel %q: %w",
			channelName,
			err,
		)
	}

	if err := client.Set(
		subCtx,
		subscriptionKey,
		callbackURL,
		0,
	).Err(); err != nil {
		cancel()
		_ = pubsub.Unsubscribe(context.Background(), channelName)
		_ = pubsub.Close()

		return nil, fmt.Errorf(
			"save subscription state for %q: %w",
			channelName,
			err,
		)
	}

	info := &subscriptionInfo{
		ID:          subscriptionID,
		Key:         subscriptionKey,
		Channel:     channelName,
		CallbackURL: callbackURL,
		Cancel:      cancel,
	}

	subscriptionsMu.Lock()
	subscriptions[subscriptionKey] = cancel
	subscriptionsMu.Unlock()

	wg.Add(1)

	go runSubscription(
		subCtx,
		pubsub,
		SubscriptionListener{
			Channel:     channelName,
			CallbackURL: callbackURL,
			Callbacks: []func(
				url string,
				message PublishRequest,
			){
				sendMessageToEndpoint,
			},
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
	defer wg.Done()

	defer func() {
		subscriptionsMu.Lock()
		delete(subscriptions, info.Key)
		subscriptionsMu.Unlock()
	}()

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := pubsub.Unsubscribe(cleanupCtx, listener.Channel); err != nil {
			fmt.Printf(
				"Error unsubscribing from channel %s: %v\n",
				listener.Channel,
				err,
			)
		}

		if err := client.Del(cleanupCtx, info.Key).Err(); err != nil {
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
				fmt.Printf("Error decoding message: %v\n", err)
				continue
			}

			for _, callback := range listener.Callbacks {
				callback(listener.CallbackURL, message)
			}

			if message.Type == "Signal" &&
				message.Message == "UNSUBSCRIBE" {
				info.Cancel()
				return
			}
		}
	}
}

func sendMessageToEndpoint(url string, message PublishRequest) {
	payload, err := json.Marshal(message)
	if err != nil {
		fmt.Printf("Error marshaling message: %v\n", err)
		return
	}

	req, err := http.NewRequest(
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

	req.Header.Set("Content-Type", "application/json")

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
	parts := strings.SplitN(key, ":", 3)
	if len(parts) < 3 {
		return ""
	}

	return parts[1]
}

func activeChannelsHandler(w http.ResponseWriter, r *http.Request) {
	channels, err := client.PubSubChannels(ctx(), "").Result()
	if err != nil {
		http.Error(
			w,
			"Error retrieving Redis channels",
			http.StatusInternalServerError,
		)
		return
	}

	respJSON, err := json.Marshal(channels)
	if err != nil {
		http.Error(
			w,
			"Error marshaling JSON",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_, _ = w.Write(respJSON)
}

func bootstrapHandler(w http.ResponseWriter, r *http.Request) {
	subscriptions := []struct {
		channel     string
		callbackURL string
	}{
		{
			channel:     "dev:global:logs:rate_limiters",
			callbackURL: os.Getenv("PUBSUB_DEFAULT_CALLBACK_URL"),
		},
		{
			channel:     "dev:global:logs:rate_limiters",
			callbackURL: os.Getenv("DEFAULT_AXIOM_URL"),
		},
		{
			channel:     "dev:global:logs:1",
			callbackURL: os.Getenv("PUBSUB_DEFAULT_CALLBACK_URL"),
		},
	}

	for _, subscription := range subscriptions {
		if subscription.callbackURL == "" {
			http.Error(
				w,
				"Bootstrap callback URL is not configured",
				http.StatusInternalServerError,
			)
			return
		}

		if _, err := startSubscription(
			subscription.channel,
			subscription.callbackURL,
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
	_, _ = w.Write([]byte("Bootstrap successful\n"))
}

func NewRouter() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(authenticateAPIKey)

	r.Route("/channels", func(r chi.Router) {
		r.Post(
			"/{channelName}/subscribe",
			subscribeHandler,
		)

		r.Get("/", activeChannelsHandler)

		r.Post(
			"/groups/save",
			saveSubscriptionGroup,
		)

		r.Get(
			"/groups/load/{groupID}",
			loadSubscriptionGroup,
		)

		r.Get(
			"/groups",
			listSubscriptionGroups,
		)
	})

	r.Get("/bootstrap", bootstrapHandler)

	return r
}

func subscribeHandler(w http.ResponseWriter, r *http.Request) {
	channelName := chi.URLParam(r, "channelName")

	var requestBody struct {
		CallbackURL string `json:"callbackURL"`
	}

	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(
			w,
			"Failed to parse request body",
			http.StatusBadRequest,
		)
		return
	}

	callbackURL := requestBody.CallbackURL

	if callbackURL == "" {
		callbackURL = os.Getenv("PUBSUB_DEFAULT_CALLBACK_URL")

		if callbackURL == "" {
			http.Error(
				w,
				"Default callback URL not set",
				http.StatusInternalServerError,
			)
			return
		}
	}

	info, err := startSubscription(channelName, callbackURL)
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
		CallbackURL:    info.CallbackURL,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		fmt.Printf("Error encoding subscription response: %v\n", err)
	}
}

func generateGroupID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func scanKeys(pattern string) ([]string, error) {
	keys := make([]string, 0)

	iter := client.Scan(
		ctx(),
		0,
		pattern,
		redisScanCount,
	).Iterator()

	for iter.Next(ctx()) {
		keys = append(keys, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return keys, nil
}

func saveSubscriptionGroup(w http.ResponseWriter, r *http.Request) {
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
		callbackURL, err := client.Get(ctx(), key).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}

			fmt.Printf(
				"Error retrieving callback URL for key %s: %v\n",
				key,
				err,
			)
			continue
		}

		parts := strings.SplitN(key, ":", 3)
		if len(parts) < 3 {
			fmt.Printf("Unexpected key format: %s\n", key)
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
			ctx(),
			subscriptionKey,
			callbackURL,
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

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)

	_, _ = w.Write([]byte(groupID))
}

func loadSubscriptionGroup(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

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
		callbackURL, err := client.Get(ctx(), key).Result()
		if err != nil {
			if err == redis.Nil {
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
			fmt.Printf("Unexpected group key format: %s\n", key)
			continue
		}

		if _, err := startSubscription(
			channelName,
			callbackURL,
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"groupID":          groupID,
		"subscriptions":   loaded,
		"message":          "Subscription group loaded",
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
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

	if !strings.HasPrefix(key, prefix) {
		return "", false
	}

	remainder := strings.TrimPrefix(key, prefix)

	parts := strings.SplitN(remainder, ":", 2)
	if len(parts) != 2 {
		return "", false
	}

	return parts[1], true
}

func listSubscriptionGroups(w http.ResponseWriter, r *http.Request) {
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

	groupSet := make(map[string]struct{})

	prefix := subscriptionGroupsPrefix + ":"

	for _, key := range groupKeys {
		remainder := strings.TrimPrefix(key, prefix)

		parts := strings.SplitN(remainder, ":", 2)
		if len(parts) == 0 || parts[0] == "" {
			continue
		}

		groupSet[parts[0]] = struct{}{}
	}

	groupIDs := make([]string, 0, len(groupSet))

	for groupID := range groupSet {
		groupIDs = append(groupIDs, groupID)
	}

	sort.Strings(groupIDs)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(groupIDs); err != nil {
		fmt.Printf(
			"Error encoding subscription group IDs: %v\n",
			err,
		)
	}
}

func stopSubscription(subscriptionKey string) bool {
	subscriptionsMu.Lock()
	cancel, ok := subscriptions[subscriptionKey]
	subscriptionsMu.Unlock()

	if !ok {
		return false
	}

	cancel()
	return true
}

func ShutdownSubscriptions(shutdownCtx context.Context) error {
	rootCancel()

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil

	case <-shutdownCtx.Done():
		return shutdownCtx.Err()
	}
}

func ctx() context.Context {
	return rootCtx
}
