package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/serverless/keyspace"
	"github.com/xd-dash/logma/serverless/router"
)

func secret(name string) (string, error) {
	file, value := os.Getenv(name+"_FILE"), os.Getenv(name)
	if file == "" {
		return value, nil
	}
	if value != "" {
		return "", errors.New("ambiguous credential input")
	}
	b, err := os.ReadFile(file)
	return strings.TrimRight(string(b), "\r\n"), err
}

func run() error {
	scope, err := keyspace.ParseScope(os.Getenv("FATLINE_SCOPE"))
	if err != nil {
		return err
	}
	opts, err := redis.ParseURL(os.Getenv("FATLINE_REDIS_URL"))
	if err != nil {
		return errors.New("invalid Fatline Redis URL")
	}
	opts.Username = os.Getenv("REDIS_USERNAME")
	opts.Password, err = secret("REDIS_PASSWORD")
	if err != nil {
		return err
	}
	token, err := secret("LOGMA_HTTP_TOKEN")
	if err != nil {
		return err
	}
	client := redis.NewClient(opts)
	defer client.Close()
	kinds := []string{"delivery.received", "execution.started", "execution.succeeded", "execution.failed", "delivery.settled"}
	channels := make([]string, len(kinds))
	for i, kind := range kinds {
		channels[i] = string(scope) + ":logma:lifecycle:probot-runtime:" + kind
	}
	handler, err := router.NewSubscriptionRouter(client, channels, token)
	if err != nil {
		return err
	}
	addr := os.Getenv("LOGMA_LISTEN_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8083"
	}
	server := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
	}()
	err = server.ListenAndServe()
	cancel()
	<-stopped
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
