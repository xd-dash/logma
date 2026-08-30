package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	ratelimiter "github.com/dash-xd/ratelimiter"
	logmalifecycle "github.com/xd-dash/logma/internal/lifecycle"
	"github.com/xd-dash/logma/internal/routes"
)

const shutdownTimeout = 10 * time.Second

func main() {
	port := getPortFromArgs()
	stateDir := os.Getenv("LOGMA_LIFECYCLE_STATE_DIR")
	if stateDir == "" {
		stateDir = "/var/lib/logma/lifecycle"
	}
	tickInterval := time.Second
	if raw := os.Getenv("LOGMA_LIFECYCLE_TICK_INTERVAL"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			panic(fmt.Errorf("parse LOGMA_LIFECYCLE_TICK_INTERVAL: %w", err))
		}
		tickInterval = parsed
	}

	redisClient, err := ratelimiter.NewClientFromEnv()
	if err != nil {
		panic(err)
	}
	defer redisClient.Close()

	lifecycleService, err := logmalifecycle.NewService(redisClient, stateDir, tickInterval)
	if err != nil {
		panic(err)
	}
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := lifecycleService.Start(rootCtx); err != nil {
		panic(err)
	}

	legacy := routes.NewRouter()
	lifecycleHandler := authenticateAPIKey(http.StripPrefix(
		"/lifecycle/api/v0.0.1",
		logmalifecycle.Handler(lifecycleService),
	))
	mux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/lifecycle/api/v0.0.1" || strings.HasPrefix(r.URL.Path, "/lifecycle/api/v0.0.1/") {
			lifecycleHandler.ServeHTTP(w, r)
			return
		}
		legacy.ServeHTTP(w, r)
	})

	server := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	serverErr := make(chan error, 1)
	go func() {
		fmt.Printf("Lifecycle Logma Server is listening on http://localhost:%d\n", port)
		serverErr <- server.ListenAndServe()
	}()

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
		return
	case <-signalCtx.Done():
	}

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("HTTP shutdown error: %v\n", err)
	}
	if err := lifecycleService.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("lifecycle shutdown error: %v\n", err)
	}
	if err := routes.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("subscription shutdown error: %v\n", err)
	}
}

func authenticateAPIKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expected := os.Getenv("API_KEY")
		provided := r.Header.Get("X-API-Key")
		if expected == "" {
			http.Error(w, "API key is not set", http.StatusInternalServerError)
			return
		}
		if len(provided) != len(expected) || subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
			http.Error(w, "Invalid API key", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func getPortFromArgs() int {
	const defaultPort = 18081
	if len(os.Args) <= 1 {
		return defaultPort
	}
	port, err := strconv.Atoi(os.Args[1])
	if err != nil || port < 1 || port > 65535 {
		fmt.Println("Invalid port provided. Using default port:", defaultPort)
		return defaultPort
	}
	return port
}
