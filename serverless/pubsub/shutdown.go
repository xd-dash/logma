package pubsub

import (
	"encoding/json"
	"log"
)

type ShutdownRequest struct {
	Reason string `json:"reason"`
}

func ParseShutdownRequest(payload string) ShutdownRequest {
	var request ShutdownRequest
	if payload == "" {
		return request
	}
	if err := json.Unmarshal([]byte(payload), &request); err != nil {
		log.Printf("pubsub: invalid shutdown message: %v", err)
	}
	return request
}
