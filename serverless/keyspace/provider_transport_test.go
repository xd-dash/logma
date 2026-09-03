package keyspace

import (
	"reflect"
	"testing"
)

func TestLogmaPubSubTransportAddressAndRequirements(t *testing.T) {
	scope, err := ParseScope("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	address, err := LogmaPubSubTransportChannel(scope, " market:quotes ")
	if err != nil {
		t.Fatal(err)
	}
	if want := "tenant-a:logma:transport:channel:market%3Aquotes"; address != want {
		t.Fatalf("transport address = %q, want %q", address, want)
	}

	tests := []struct {
		name       string
		access     Access
		commands   []string
		notCommand string
	}{
		{name: "publish", access: AccessPublish, commands: []string{"client", "hello", "ping", "publish"}, notCommand: "subscribe"},
		{name: "subscribe", access: AccessSubscribe, commands: []string{"client", "hello", "ping", "subscribe", "unsubscribe"}, notCommand: "publish"},
		{name: "both", access: AccessPublish | AccessSubscribe, commands: []string{"client", "hello", "ping", "publish", "subscribe", "unsubscribe"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := CompileRedisRequirements(scope, Grant{Capability: CapabilityLogmaPubSubTransport, Access: tt.access})
			if err != nil {
				t.Fatal(err)
			}
			if len(req.KeyPatterns) != 0 {
				t.Fatalf("transport unexpectedly grants keys: %v", req.KeyPatterns)
			}
			if want := []string{"&tenant-a:logma:transport:channel:*"}; !reflect.DeepEqual(req.ChannelPatterns, want) {
				t.Fatalf("channel patterns = %v, want %v", req.ChannelPatterns, want)
			}
			if !reflect.DeepEqual(req.Commands, tt.commands) {
				t.Fatalf("commands = %v, want %v", req.Commands, tt.commands)
			}
			if tt.notCommand != "" {
				for _, command := range req.Commands {
					if command == tt.notCommand {
						t.Fatalf("transport %s unexpectedly grants %s", tt.name, tt.notCommand)
					}
				}
			}
		})
	}
}

func TestLogmaPubSubTransportRejectsGraphAccess(t *testing.T) {
	scope, _ := ParseScope("tenant-a")
	for _, access := range []Access{AccessRead, AccessWrite, AccessPublish | AccessRead, 0} {
		if _, err := CompileRedisRequirements(scope, Grant{Capability: CapabilityLogmaPubSubTransport, Access: access}); err == nil {
			t.Fatalf("transport access %d unexpectedly compiled", access)
		}
	}
	if _, err := CompileRedisRequirements(scope, Grant{Capability: CapabilityLogmaPubSubGraph, Access: AccessPublish}); err == nil {
		t.Fatal("graph capability unexpectedly accepted Publish")
	}
}
