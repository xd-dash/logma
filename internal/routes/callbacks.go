package routes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/redis/go-redis/v9"
)

const callbackConfigVersion = "v1"

type callbackScheme struct {
	Type    string            `json:"type"`
	URL     string            `json:"url,omitempty"`
	URLs    []string          `json:"urls,omitempty"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Config  json.RawMessage   `json:"config,omitempty"`
}

type callbackConfig struct {
	Version   string           `json:"version,omitempty"`
	Callbacks []callbackScheme `json:"callbacks"`
}

type callbackInput struct {
	CallbackURL  json.RawMessage `json:"callbackURL,omitempty"`
	CallbackURLs []string        `json:"callbackURLs,omitempty"`
	Callbacks    json.RawMessage `json:"callbacks,omitempty"`
}

func singleHTTPCallbackConfig(url string) callbackConfig {
	return callbackConfig{
		Version: callbackConfigVersion,
		Callbacks: []callbackScheme{
			{
				Type: "http",
				URL:  strings.TrimSpace(url),
			},
		},
	}
}

func parseCallbackConfigFromRequest(r *http.Request) (callbackConfig, error) {
	var config callbackConfig

	queryURLs := callbackURLsFromQuery(r)
	if len(queryURLs) > 0 {
		for _, url := range queryURLs {
			config.Callbacks = append(
				config.Callbacks,
				callbackScheme{
					Type: "http",
					URL:  url,
				},
			)
		}
		config.Version = callbackConfigVersion
		return normalizeCallbackConfig(config)
	}

	if r.Body == nil {
		return config, nil
	}

	var input callbackInput
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&input); err != nil {
		if errors.Is(err, io.EOF) {
			return config, nil
		}
		return config, errors.New("Failed to parse request body")
	}

	return callbackConfigFromInput(input)
}

func callbackConfigFromInput(
	input callbackInput,
) (callbackConfig, error) {
	var config callbackConfig

	if len(input.CallbackURL) > 0 {
		urls, err := decodeStringOrStringList(input.CallbackURL)
		if err != nil {
			return config, fmt.Errorf("callbackURL: %w", err)
		}
		for _, url := range urls {
			config.Callbacks = append(
				config.Callbacks,
				callbackScheme{
					Type: "http",
					URL:  url,
				},
			)
		}
	}

	for _, url := range input.CallbackURLs {
		config.Callbacks = append(
			config.Callbacks,
			callbackScheme{
				Type: "http",
				URL:  strings.TrimSpace(url),
			},
		)
	}

	if len(input.Callbacks) > 0 {
		schemes, err := decodeCallbackSchemes(input.Callbacks)
		if err != nil {
			return config, fmt.Errorf("callbacks: %w", err)
		}
		config.Callbacks = append(
			config.Callbacks,
			schemes...,
		)
	}

	if len(config.Callbacks) > 0 {
		config.Version = callbackConfigVersion
	}

	return normalizeCallbackConfig(config)
}

func callbackURLsFromQuery(r *http.Request) []string {
	values := make([]string, 0)

	for _, key := range []string{
		"callbackURL",
		"callback",
		"callback_url",
	} {
		for _, value := range r.URL.Query()[key] {
			value = strings.TrimSpace(value)
			if value != "" {
				values = append(values, value)
			}
		}
	}

	return values
}

func decodeStringOrStringList(raw json.RawMessage) ([]string, error) {
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return []string{single}, nil
	}

	var multiple []string
	if err := json.Unmarshal(raw, &multiple); err == nil {
		return multiple, nil
	}

	return nil, errors.New("must be a string or list of strings")
}

func decodeCallbackSchemes(raw json.RawMessage) ([]callbackScheme, error) {
	var single callbackScheme
	if err := json.Unmarshal(raw, &single); err == nil && single.Type != "" {
		return []callbackScheme{single}, nil
	}

	var multiple []callbackScheme
	if err := json.Unmarshal(raw, &multiple); err == nil {
		return multiple, nil
	}

	return nil, errors.New("must be an object or list of objects")
}

func normalizeCallbackConfig(config callbackConfig) (callbackConfig, error) {
	out := callbackConfig{
		Version: callbackConfigVersion,
	}

	for _, callback := range config.Callbacks {
		callback.Type = strings.TrimSpace(callback.Type)
		callback.URL = strings.TrimSpace(callback.URL)
		callback.Method = strings.TrimSpace(callback.Method)

		urls := make([]string, 0, len(callback.URLs))
		for _, url := range callback.URLs {
			if url = strings.TrimSpace(url); url != "" {
				urls = append(urls, url)
			}
		}
		callback.URLs = urls

		if callback.Type == "" {
			callback.Type = "http"
		}

		switch callback.Type {
		case "http":
			if callback.URL == "" && len(callback.URLs) == 0 {
				return callbackConfig{}, errors.New(
					"http callback requires url or urls",
				)
			}
			if callback.Method == "" {
				callback.Method = http.MethodPost
			}
		case "redis-function", "lua":
			var cfg redisFunctionCallbackConfig
			if len(callback.Config) == 0 ||
				json.Unmarshal(callback.Config, &cfg) != nil ||
				strings.TrimSpace(cfg.Name) == "" {
				return callbackConfig{}, errors.New(
					"redis-function callback requires config.name",
				)
			}
			callback.Type = "redis-function"
		default:
			if len(callback.Config) == 0 &&
				callback.URL == "" &&
				len(callback.URLs) == 0 {
				return callbackConfig{}, fmt.Errorf(
					"callback type %q requires configuration",
					callback.Type,
				)
			}
		}

		out.Callbacks = append(out.Callbacks, callback)
	}

	return out, nil
}

func callbackConfigEmpty(config callbackConfig) bool {
	return len(config.Callbacks) == 0
}

func encodeStoredCallbackConfig(config callbackConfig) (string, error) {
	config, err := normalizeCallbackConfig(config)
	if err != nil {
		return "", err
	}

	// Preserve the historical Redis representation for the common
	// single-URL HTTP callback.
	if len(config.Callbacks) == 1 {
		callback := config.Callbacks[0]
		if callback.Type == "http" &&
			callback.URL != "" &&
			len(callback.URLs) == 0 &&
			(callback.Method == "" ||
				callback.Method == http.MethodPost) &&
			len(callback.Headers) == 0 &&
			len(callback.Config) == 0 {
			return callback.URL, nil
		}
	}

	encoded, err := json.Marshal(config)
	if err != nil {
		return "", err
	}

	return string(encoded), nil
}

func decodeStoredCallbackConfig(value string) (callbackConfig, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return callbackConfig{}, errors.New("empty callback configuration")
	}

	if !strings.HasPrefix(value, "{") {
		return singleHTTPCallbackConfig(value), nil
	}

	var config callbackConfig
	if err := json.Unmarshal([]byte(value), &config); err != nil {
		return callbackConfig{}, err
	}

	return normalizeCallbackConfig(config)
}

func dispatchCallbackConfig(
	dispatcher *callbackDispatcher,
	config callbackConfig,
	message PublishRequest,
	tenant string,
	redisClient *redis.Client,
) {
	for _, callback := range config.Callbacks {
		switch callback.Type {
		case "http":
			urls := make([]string, 0, 1+len(callback.URLs))
			if callback.URL != "" {
				urls = append(urls, callback.URL)
			}
			urls = append(urls, callback.URLs...)

			for _, url := range urls {
				dispatcher.dispatch(
					func(target string, message PublishRequest) {
						sendMessageToEndpointWithScheme(
							target,
							callback,
							message,
						)
					},
					url,
					message,
				)
			}
		case "redis-function":
			dispatchRedisFunctionCallback(
				dispatcher,
				callback,
				message,
				tenant,
				redisClient,
			)
		}
	}
}

func sendMessageToEndpointWithScheme(
	url string,
	callback callbackScheme,
	message PublishRequest,
) {
	payload, err := json.Marshal(message)
	if err != nil {
		fmt.Printf("Error marshaling message: %v\n", err)
		return
	}

	reqCtx, cancel := context.WithTimeout(
		context.Background(),
		callbackTimeout,
	)
	defer cancel()

	method := callback.Method
	if method == "" {
		method = http.MethodPost
	}

	req, err := http.NewRequestWithContext(
		reqCtx,
		method,
		url,
		bytes.NewReader(payload),
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
	for key, value := range callback.Headers {
		req.Header.Set(key, value)
	}

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
