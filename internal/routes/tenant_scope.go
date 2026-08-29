package routes

import (
	"errors"
	"net/http"
	"strings"

	logmaacl "github.com/xd-dash/logma/acl"
)

func scopeChannelForPrincipal(principal requestPrincipal, channel string) (string, error) {
	channel = strings.TrimSpace(channel)
	if channel == "" {
		return "", errors.New("channel name is required")
	}
	if principal.Admin || principal.Tenant == "" {
		return channel, nil
	}

	prefix := logmaacl.TenantChannelPrefix(principal.Tenant)
	if strings.HasPrefix(channel, prefix) {
		return channel, nil
	}
	if strings.HasPrefix(channel, "tenant:") {
		return "", errors.New("channel belongs to another tenant namespace")
	}
	return prefix + channel, nil
}

func managedTenantGroupsAllowed(principal requestPrincipal) bool {
	// Groups still use the historical global Redis key schema. Keep them
	// admin-only in managed mode until their storage keys are tenant-prefixed.
	return principal.Admin
}

func managedGroupGuard(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if authProfileFromEnv().Name != "managed" {
			handler(w, r)
			return
		}
		principal, ok := principalFromRequest(r)
		if !ok || !managedTenantGroupsAllowed(principal) {
			http.Error(
				w,
				"subscription groups are admin-only in managed mode until group storage is tenant-namespaced",
				http.StatusForbidden,
			)
			return
		}
		handler(w, r)
	}
}
