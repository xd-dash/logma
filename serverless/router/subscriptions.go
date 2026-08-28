package router

import (
	"github.com/xd-dash/logma/serverless/internal/addsub"
	"github.com/xd-dash/logma/serverless/internal/shutdown"
	"github.com/xd-dash/logma/serverless/pubsub"
)

type Handle func(session *pubsub.Session, payload string, add func(channel string) error)

var Subscriptions = make(map[string]Handle)

func RegisterChannel(channel string, handle Handle) {
	Subscriptions[channel] = handle
}

func init() {
	RegisterChannel(addsub.Channel, addsub.Handle)
	RegisterChannel(shutdown.Channel, shutdown.Handle)
}
