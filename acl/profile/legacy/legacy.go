// Package legacy exposes the backwards-compatible single-credential Logma profile.
package legacy

import "github.com/xd-dash/logma/acl"

func New() acl.Profile {
	return acl.Profile{
		Name:    "legacy",
		Managed: false,
	}
}
