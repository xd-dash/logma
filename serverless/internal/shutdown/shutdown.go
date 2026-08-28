package shutdown

import (
	"log"

	"github.com/xd-dash/logma/serverless/pubsub"
	"github.com/xd-dash/logma/serverless/pubsub/channels"
)

var Channel = channels.Discover().ShutdownChannel()

func Handle(session *pubsub.Session, payload string, _ func(channel string) error) {
	request := pubsub.ParseShutdownRequest(payload)
	log.Printf("shutdown: reason=%q", request.Reason)
	session.Cancel()
}
