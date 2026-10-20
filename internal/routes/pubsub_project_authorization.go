package routes

import (
 "context"
 "net/http"
 "strings"

)

// ProjectBindingResolver resolves verified authority established by a trusted
// identity provider. It MUST NOT trust project or principal values from
// unverified request headers. The resolver must reject revoked bindings.
type ProjectBindingResolver interface {
 ResolveProjectBinding(context.Context) (VerifiedProjectBinding, error)
}

// VerifiedProjectBinding contains only authority verified by the resolver.
// Authorize must apply Prajapati's bound policy, including exact audience,
// action and project/resource checks.
type VerifiedProjectBinding interface {
 Authorize(action, resource string) error
 ProjectID() string
}

type projectBindingContextKey struct{}

func projectAuthorizationMiddleware(resolver ProjectBindingResolver) func(http.Handler) http.Handler {
 return func(next http.Handler) http.Handler {
  return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
   if resolver == nil { http.Error(w, "project authorization is unavailable", http.StatusServiceUnavailable); return }
   binding, err := resolver.ResolveProjectBinding(r.Context())
   if err != nil || binding == nil || strings.TrimSpace(binding.ProjectID()) == "" {
    http.Error(w, "verified project binding required", http.StatusForbidden)
    return
   }
   next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), projectBindingContextKey{}, binding)))
  })
 }
}

// authorizeProjectResource enforces the principal's verified binding for every
// resource operation. Resource identifiers are project-qualified by the
// immutable project identity; callers cannot select another project by header.
func authorizeProjectResource(w http.ResponseWriter, r *http.Request, action, kind, id string) bool {
 binding, ok := r.Context().Value(projectBindingContextKey{}).(VerifiedProjectBinding)
 if !ok || binding == nil || binding.ProjectID() == "" {
  http.Error(w, "verified project binding required", http.StatusForbidden); return false
 }
 if strings.TrimSpace(id) == "" { id = "collection" }
 resource := "project/" + binding.ProjectID() + "/pubsub/" + kind + "/" + id
 if strings.Contains(binding.ProjectID(), "/") || strings.Contains(id, "/") {
  http.Error(w, "project access denied", http.StatusForbidden); return false
 }
 if err := binding.Authorize(action, resource); err != nil {
  http.Error(w, "project access denied", http.StatusForbidden); return false
 }
 return true
}
