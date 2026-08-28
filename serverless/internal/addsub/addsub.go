package addsub

import (
	"encoding/json"
	"log"

	"github.com/xd-dash/logma/serverless/pubsub"
	"github.com/xd-dash/logma/serverless/pubsub/channels"
)

var Channel = channels.Discover().AddChannel()

type Request struct {
	Channel string `json:"channel"`
}

func Handle(_ *pubsub.Session, payload string, add func(channel string) error) {
	var request Request
	if err := json.Unmarshal([]byte(payload), &request); err != nil {
		log.Printf("addsub: invalid message: %v", err)
		return
	}
	if request.Channel == "" {
		log.Printf("addsub: message contained empty channel")
		return
	}
	if err := add(request.Channel); err != nil {
		log.Printf("addsub: failed to add subscription %q: %v", request.Channel, err)
	}
}
