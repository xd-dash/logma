package pubsub

import (
	"encoding/json"
	"log"
)

type ShutdownRequest struct {
	Reason string `json:"reason"`
}

type lifecycleSignalEnvelope struct {
	Type    string `json:"type"`
	Message struct {
		Signal    string `json:"signal"`
		Condition string `json:"condition"`
	} `json:"message"`
}

func ParseShutdownRequest(payload string) ShutdownRequest {
	var request ShutdownRequest
	if payload == "" {
		return request
	}
	if err := json.Unmarshal([]byte(payload), &request); err != nil {
		log.Printf("pubsub: invalid shutdown message: %v", err)
		return ShutdownRequest{}
	}
	if request.Reason != "" {
		return request
	}

	var signal lifecycleSignalEnvelope
	if err := json.Unmarshal([]byte(payload), &signal); err != nil {
		return request
	}
	if signal.Type == "Signal" && signal.Message.Signal == "shutdown" {
		request.Reason = "lifecycle:" + signal.Message.Condition
	}
	return request
}
