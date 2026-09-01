package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/xd-dash/logma/internal/routes"
)

const shutdownTimeout = 10 * time.Second

func main() {
	lifecycleRouter, err := routes.NewLifecycleRouter()
	if err != nil {
		panic(fmt.Errorf("initialize lifecycle API: %w", err))
	}

	router := chi.NewRouter()
	router.Mount("/lifecycle", lifecycleRouter)
	router.Mount("/", routes.NewRouter())

	port := getPortFromArgs()
	addr := net.JoinHostPort(getBindHost(), strconv.Itoa(port))

	server := &http.Server{
		Addr:    addr,
		Handler: router,
	}

	serverErr := make(chan error, 1)
	go func() {
		fmt.Printf("Go Server is listening on http://%s\n", addr)
		serverErr <- server.ListenAndServe()
	}()

	signalCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
		return
	case <-signalCtx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		shutdownTimeout,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("HTTP shutdown error: %v\n", err)
	}

	if err := routes.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("subscription shutdown error: %v\n", err)
	}
}

func getBindHost() string {
	host := strings.TrimSpace(os.Getenv("LOGMA_BIND_ADDR"))
	if host == "" {
		return "0.0.0.0"
	}
	return host
}

func getPortFromArgs() int {
	defaultPort := 8080
	if len(os.Args) > 1 {
		port, err := strconv.Atoi(os.Args[1])
		if err != nil {
			fmt.Println("Invalid port provided. Using default port:", defaultPort)
			return defaultPort
		}
		return port
	}
	return defaultPort
}
