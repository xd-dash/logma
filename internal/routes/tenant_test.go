package routes

import (
	"net/http/httptest"
	"testing"
)

func TestTenantIDFromRequestUsesHeader(t *testing.T) {
	t.Setenv("LOGMA_DEFAULT_TENANT_ID", "default")
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(tenantIDHeader, "tenant-a")

	got, err := tenantIDFromRequest(req)
	if err != nil {
		t.Fatalf("tenantIDFromRequest() error = %v", err)
	}
	if got != "tenant-a" {
		t.Fatalf("tenantIDFromRequest() = %q, want tenant-a", got)
	}
}

func TestTenantIDFromRequestUsesDefault(t *testing.T) {
	t.Setenv("LOGMA_DEFAULT_TENANT_ID", "default")
	req := httptest.NewRequest("GET", "/", nil)

	got, err := tenantIDFromRequest(req)
	if err != nil {
		t.Fatalf("tenantIDFromRequest() error = %v", err)
	}
	if got != "default" {
		t.Fatalf("tenantIDFromRequest() = %q, want default", got)
	}
}

func TestTenantIDRejectsDelimiter(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(tenantIDHeader, "tenant:a")

	if _, err := tenantIDFromRequest(req); err == nil {
		t.Fatal("tenantIDFromRequest() expected error")
	}
}

func TestCompactKeyPatterns(t *testing.T) {
	if got := activeSubscriptionPattern("t1", "42", "dev:logs"); got != "as:t1:42:dev:logs" {
		t.Fatalf("activeSubscriptionPattern() = %q", got)
	}
	if got := subscriptionGroupPattern("t1", "7", "42", "dev:logs"); got != "sg:t1:7:42:dev:logs" {
		t.Fatalf("subscriptionGroupPattern() = %q", got)
	}
	if got := subscriptionGroupPattern("t1", "7", "", ""); got != "sg:t1:7:" {
		t.Fatalf("subscriptionGroupPattern() prefix = %q", got)
	}
}

func TestSubscriptionFromKeyParts(t *testing.T) {
	got, err := subscriptionFromKeyParts("as:t1:42:dev:global:logs", "https://example.test")
	if err != nil {
		t.Fatalf("subscriptionFromKeyParts() error = %v", err)
	}
	if got.TenantID != "t1" || got.ID != "42" || got.Channel != "dev:global:logs" {
		t.Fatalf("subscriptionFromKeyParts() = %#v", got)
	}
}
