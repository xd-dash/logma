package keyspace

import "strings"

// LogmaPubSubTransportFamily is the provider grammar for Redis Pub/Sub topics
// carrying Logma Channel traffic. It is intentionally distinct from the
// durable graph family while sharing the same explicit FATLINE scope.
func LogmaPubSubTransportFamily(scope Scope) (Family, error) {
	return NewFamily(scope, "logma", "transport", "channel")
}

// LogmaPubSubTransportChannel converts one logical Logma Channel identity into
// its canonical scoped Redis Pub/Sub topic. Opaque identity encoding is the
// same injective encoding used for durable resource addresses.
func LogmaPubSubTransportChannel(scope Scope, logicalChannel string) (string, error) {
	logicalChannel = strings.TrimSpace(logicalChannel)
	family, err := LogmaPubSubTransportFamily(scope)
	if err != nil {
		return "", err
	}
	resource, err := family.Resource(logicalChannel)
	if err != nil {
		return "", err
	}
	return resource.Key(), nil
}
