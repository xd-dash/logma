package lifecycle

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestLifecycleKeyspaceUsesFatlineScope(t *testing.T) {
	got, scope, err := lifecycleKeyspace("fatline-prod")
	if err != nil {
		t.Fatal(err)
	}
	if got != "fatline-prod:logma:lifecycle" || scope != "fatline-prod" {
		t.Fatalf("lifecycleKeyspace() = (%q, %q), want scoped keyspace", got, scope)
	}
}

func TestLifecycleKeyspaceRejectsInvalidScope(t *testing.T) {
	if _, _, err := lifecycleKeyspace("foreign scope"); err == nil {
		t.Fatal("lifecycleKeyspace accepted invalid scope")
	}
}

func TestLifecycleKeyspaceRetainsUnscopedCompatibility(t *testing.T) {
	got, scope, err := lifecycleKeyspace("")
	if err != nil {
		t.Fatal(err)
	}
	if got != defaultKeyspace || scope != "" {
		t.Fatalf("lifecycleKeyspace() = (%q, %q), want legacy default", got, scope)
	}
}

func TestValidateShutdownChannelRequiresMatchingScope(t *testing.T) {
	runtime := &Runtime{scope: "fatline-prod"}
	if err := runtime.validateShutdownChannel("fatline-prod:tenant:shutdown"); err != nil {
		t.Fatalf("scoped channel rejected: %v", err)
	}
	if err := runtime.validateShutdownChannel("other:tenant:shutdown"); err == nil {
		t.Fatal("foreign channel accepted")
	}
}


func TestReadinessClientUsesBoundedContextAwareIO(t *testing.T) {
	application := redis.NewClient(&redis.Options{
		Network:  "unix",
		Addr:     "/run/redis/redis.sock",
		Username: "logma",
		Password: "qualification-password",
		ReadTimeout: 5 * time.Second,
	})
	defer application.Close()

	readiness := newReadinessClient(application)
	defer readiness.Close()
	options := readiness.Options()
	if !options.ContextTimeoutEnabled {
		t.Fatal("readiness client must enforce request contexts")
	}
	if options.DialTimeout != readinessRedisIOTimeout ||
		options.ReadTimeout != readinessRedisIOTimeout ||
		options.WriteTimeout != readinessRedisIOTimeout {
		t.Fatalf("readiness IO timeouts = (%s, %s, %s), want %s",
			options.DialTimeout, options.ReadTimeout, options.WriteTimeout, readinessRedisIOTimeout)
	}
	if options.Network != "unix" || options.Addr != "/run/redis/redis.sock" ||
		options.Username != "logma" || options.Password != "qualification-password" {
		t.Fatalf("readiness client lost endpoint or credentials: %+v", options)
	}
	if application.Options().ContextTimeoutEnabled ||
		application.Options().ReadTimeout != 5*time.Second {
		t.Fatal("creating readiness client mutated the application Redis client")
	}
}
