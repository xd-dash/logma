package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/xd-dash/logma/internal/routes"
)

const shutdownTimeout = 10 * time.Second
const lifecyclePrefix = "/lifecycle/api/v0.0.1"

func main() {
	port := getPortFromArgs()
	lifecycleRouter, err := routes.NewLifecycleRouter()
	if err != nil {
		panic(fmt.Errorf("initialize lifecycle API: %w", err))
	}

	legacy := routes.NewRouter()
	lifecycleHandler := http.StripPrefix(lifecyclePrefix, lifecycleRouter)
	mux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == lifecyclePrefix || strings.HasPrefix(r.URL.Path, lifecyclePrefix+"/") {
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

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("HTTP shutdown error: %v\n", err)
	}
	if err := routes.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("subscription shutdown error: %v\n", err)
	}
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
