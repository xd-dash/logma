// Package managed exposes the Redis-ACL-backed multi-tenant Logma profile.
package managed

import "github.com/xd-dash/logma/acl"

const DefaultAdminUser = "logma-admin"

func New(adminUser string) acl.Profile {
	if adminUser == "" {
		adminUser = DefaultAdminUser
	}
	base, _ := acl.PolicyByName("tenant")
	return acl.Profile{
		Name:       "managed",
		Managed:    true,
		AdminUser:  adminUser,
		TenantBase: base,
	}
}
