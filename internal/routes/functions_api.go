package routes

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/dash-xd/ratelimiter/auth"
	"github.com/go-chi/chi/v5"
)

const aclFunctionMetadataPrefix = "logma:acl:function:"

type tenantFunctionRequest struct {
	Tenant   string `json:"tenant,omitempty"`
	Name     string `json:"name"`
	Source   string `json:"source"`
	ReadOnly bool   `json:"readOnly,omitempty"`
}

type tenantFunctionRecord struct {
	Tenant   string `json:"tenant"`
	Name     string `json:"name"`
	Function string `json:"function"`
	Library  string `json:"library"`
	ReadOnly bool   `json:"readOnly"`
}

func functionMetadataKey(tenant, name string) string {
	return aclFunctionMetadataPrefix + tenant + ":" + name
}

func uploadTenantFunctionHandler(w http.ResponseWriter, r *http.Request) {
	principal, ok := principalFromRequest(r)
	if func() bool { p, _ := authProviderFromEnv(); return p == nil }() || !ok {
		http.Error(w, "Tenant functions require managed auth profile", http.StatusNotFound)
		return
	}

	var request tenantFunctionRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	tenant := principal.Tenant
	if principal.Admin {
		tenant = strings.TrimSpace(request.Tenant)
	}
	if err := auth.ValidateIdentifier(tenant); err != nil {
		http.Error(w, "Invalid tenant: "+err.Error(), http.StatusBadRequest)
		return
	}
	if !principal.Admin && !principal.Policy.Has(auth.CapabilityFunctions) {
		http.Error(w, "This ACL profile does not allow function callbacks", http.StatusForbidden)
		return
	}

	name := strings.TrimSpace(request.Name)
	functionName, err := func() (string, error) {
		provider, err := authProviderFromEnv()
		if err != nil || provider == nil {
			return "", errors.New("auth provider unavailable")
		}
		scope, err := provider.Scope(tenant, "")
		if err != nil {
			return "", err
		}
		return auth.FunctionName(scope, name)
	}()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	libraryName, err := func() (string, error) {
		provider, err := authProviderFromEnv()
		if err != nil || provider == nil {
			return "", errors.New("auth provider unavailable")
		}
		scope, err := provider.Scope(tenant, "")
		if err != nil {
			return "", err
		}
		return auth.LibraryName(scope, name)
	}()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	source := strings.TrimSpace(request.Source)
	if source == "" {
		http.Error(w, "source is required", http.StatusBadRequest)
		return
	}
	if strings.Contains(source, "#!lua") ||
		strings.Contains(source, "redis.register_function") {
		http.Error(
			w,
			"source is a function body; library metadata and registration are owned by Logma",
			http.StatusBadRequest,
		)
		return
	}

	library := buildTenantFunctionLibrary(
		libraryName,
		functionName,
		source,
		request.ReadOnly,
	)

	// Tenants never receive FUNCTION LOAD. The application-admin connection
	// brokers the global Redis library namespace and assigns canonical names.
	if err := client.Do(r.Context(), "FUNCTION", "LOAD", "REPLACE", library).Err(); err != nil {
		http.Error(w, "Failed to load Redis function: "+err.Error(), http.StatusBadRequest)
		return
	}

	record := tenantFunctionRecord{
		Tenant:   tenant,
		Name:     name,
		Function: functionName,
		Library:  libraryName,
		ReadOnly: request.ReadOnly,
	}
	raw, _ := json.Marshal(record)
	if err := client.Set(r.Context(), functionMetadataKey(tenant, name), raw, 0).Err(); err != nil {
		_ = client.Do(r.Context(), "FUNCTION", "DELETE", libraryName).Err()
		http.Error(w, "Function loaded but metadata persistence failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(record)
}

func buildTenantFunctionLibrary(libraryName, functionName, source string, readOnly bool) string {
	var registration string
	if readOnly {
		registration = fmt.Sprintf(
			"redis.register_function{function_name=%q, callback=logma_callback, flags={'no-writes'}}",
			functionName,
		)
	} else {
		registration = fmt.Sprintf(
			"redis.register_function(%q, logma_callback)",
			functionName,
		)
	}

	return fmt.Sprintf(
		"#!lua name=%s\nlocal function logma_callback(keys, args)\n%s\nend\n%s\n",
		libraryName,
		source,
		registration,
	)
}

func listTenantFunctionsHandler(w http.ResponseWriter, r *http.Request) {
	principal, ok := principalFromRequest(r)
	if func() bool { p, _ := authProviderFromEnv(); return p == nil }() || !ok {
		http.Error(w, "Tenant functions require managed auth profile", http.StatusNotFound)
		return
	}

	tenant := principal.Tenant
	if principal.Admin {
		tenant = strings.TrimSpace(r.URL.Query().Get("tenant"))
		if tenant == "" {
			http.Error(w, "tenant query parameter is required for admin listing", http.StatusBadRequest)
			return
		}
	}
	if err := auth.ValidateIdentifier(tenant); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	keys, err := scanKeys(aclFunctionMetadataPrefix + tenant + ":*")
	if err != nil {
		http.Error(w, "Failed to list functions", http.StatusInternalServerError)
		return
	}
	records := make([]tenantFunctionRecord, 0, len(keys))
	for _, key := range keys {
		raw, err := client.Get(r.Context(), key).Result()
		if err != nil {
			continue
		}
		var record tenantFunctionRecord
		if json.Unmarshal([]byte(raw), &record) == nil {
			records = append(records, record)
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Name < records[j].Name })

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(records)
}

func deleteTenantFunctionHandler(w http.ResponseWriter, r *http.Request) {
	principal, ok := principalFromRequest(r)
	if func() bool { p, _ := authProviderFromEnv(); return p == nil }() || !ok {
		http.Error(w, "Tenant functions require managed auth profile", http.StatusNotFound)
		return
	}

	name := strings.TrimSpace(chi.URLParam(r, "name"))
	tenant := principal.Tenant
	if principal.Admin {
		tenant = strings.TrimSpace(r.URL.Query().Get("tenant"))
	}
	if err := auth.ValidateIdentifier(tenant); err != nil {
		http.Error(w, "Invalid tenant", http.StatusBadRequest)
		return
	}
	if !principal.Admin && !principal.Policy.Has(auth.CapabilityFunctions) {
		http.Error(w, "This ACL profile does not allow functions", http.StatusForbidden)
		return
	}

	raw, err := client.Get(r.Context(), functionMetadataKey(tenant, name)).Result()
	if err != nil {
		http.Error(w, "Function not found", http.StatusNotFound)
		return
	}
	var record tenantFunctionRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		http.Error(w, "Invalid function metadata", http.StatusInternalServerError)
		return
	}

	if err := client.Do(r.Context(), "FUNCTION", "DELETE", record.Library).Err(); err != nil {
		http.Error(w, "Failed to delete Redis function", http.StatusInternalServerError)
		return
	}
	_ = client.Del(r.Context(), functionMetadataKey(tenant, name)).Err()
	w.WriteHeader(http.StatusNoContent)
}

func validateTenantFunctionCallback(principal requestPrincipal, callback callbackScheme) error {
	if callback.Type != "redis-function" && callback.Type != "lua" {
		return nil
	}
	if principal.Admin {
		return errors.New("admin subscriptions must specify tenant execution through a tenant identity")
	}
	if !principal.Policy.Has(auth.CapabilityFunctions) {
		return errors.New("tenant ACL profile does not allow function callbacks")
	}
	var cfg redisFunctionCallbackConfig
	if err := json.Unmarshal(callback.Config, &cfg); err != nil {
		return errors.New("redis-function callback config must be an object")
	}
	if _, err := func() (string, error) {
		provider, err := authProviderFromEnv()
		if err != nil || provider == nil {
			return "", errors.New("auth provider unavailable")
		}
		scope, err := provider.Scope(principal.Tenant, principal.Username)
		if err != nil {
			return "", err
		}
		return auth.FunctionName(scope, cfg.Name)
	}(); err != nil {
		return err
	}
	return nil
}
