package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	axiomcallback "github.com/xd-dash/logma/serverless/callbacks/axiom"
	"github.com/xd-dash/logma/serverless/pubsub"
)

func channelsFromEnv() []string {
	seen := map[string]struct{}{}
	var channels []string
	for _, raw := range strings.Split(os.Getenv("LOGMA_SUBSCRIBE_CHANNELS"), ",") {
		channel := strings.TrimSpace(raw)
		if channel == "" {
			continue
		}
		if _, ok := seen[channel]; ok {
			continue
		}
		seen[channel] = struct{}{}
		channels = append(channels, channel)
	}
	return channels
}

func main() {
	channels := channelsFromEnv()
	if len(channels) == 0 {
		fmt.Fprintln(os.Stderr, "LOGMA_SUBSCRIBE_CHANNELS must contain at least one exact channel")
		os.Exit(2)
	}
	required, _ := strconv.ParseBool(os.Getenv("LOGMA_AXIOM_REQUIRED"))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	client := pubsub.NewClientFromEnv()
	defer client.Close()
	observer := axiomcallback.FromEnv()

	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := client.Ping(pingCtx).Err(); err != nil {
		pingCancel()
		fmt.Fprintf(os.Stderr, "redis ping failed: %v\n", err)
		os.Exit(1)
	}
	pingCancel()
	fmt.Println("redis_ping=ok")

	subs := make([]*pubsub.Subscriber, 0, len(channels))
	for _, channel := range channels {
		subs = append(subs, pubsub.SubscribeObserved(ctx, client, channel, observer))
	}

	readyCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for i, sub := range subs {
		select {
		case <-sub.Ready():
		case <-readyCtx.Done():
			fmt.Fprintf(os.Stderr, "subscriber %d did not become ready: %v", i, readyCtx.Err())
			if err := sub.LastError(); err != nil {
				fmt.Fprintf(os.Stderr, "; redis error: %v", err)
			}
			fmt.Fprintln(os.Stderr)
			os.Exit(1)
		}
	}
	fmt.Printf("ready_channels=%d\n", len(subs))

	<-ctx.Done()
	for _, sub := range subs {
		select {
		case <-sub.Stopped():
		case <-time.After(2 * time.Second):
		}
	}
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := observer.Close(flushCtx)
	flushCancel()
	fmt.Printf("axiom_sent=%d axiom_failed=%d axiom_dropped=%d\n", observer.Sent(), observer.Failed(), observer.Dropped())
	if required {
		if err != nil {
			fmt.Fprintf(os.Stderr, "required Axiom flush failed: %v\n", err)
			os.Exit(1)
		}
		if observer.Sent() == 0 || observer.Failed() != 0 || observer.Dropped() != 0 {
			fmt.Fprintln(os.Stderr, "required Axiom subscriber delivery was not lossless")
			os.Exit(1)
		}
	}
}
