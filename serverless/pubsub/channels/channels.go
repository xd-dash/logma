// Package channels derives service-scoped control-plane channel names.
package channels

import (
	"os"
	"path"
	"runtime/debug"
)

type Defaults struct {
	Namespace string
}

func ForNamespace(namespace string) Defaults {
	return Defaults{Namespace: namespace}
}

func Discover() Defaults {
	if service := os.Getenv("K_SERVICE"); service != "" {
		return Defaults{Namespace: service}
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Path != "" {
		return Defaults{Namespace: path.Base(info.Main.Path)}
	}
	return Defaults{}
}

func (d Defaults) ShutdownChannel() string { return d.channel("shutdown") }
func (d Defaults) AddChannel() string      { return d.channel("add") }

func (d Defaults) channel(purpose string) string {
	if d.Namespace == "" {
		return "control:" + purpose
	}
	return d.Namespace + ":control:" + purpose
}
