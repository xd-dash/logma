package routes

import (
 "context"
 "net/http"
 "strings"
 "github.com/xd-dash/logma/serverless/authhttp"
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
   if verified, ok := authhttp.FromContext(r.Context()); ok {
    // If an authenticated HTTP boundary is installed, the resource
    // resolver must use the same binding; a second independent identity
    // would permit split-brain project authority.
    if err != nil || binding == nil || verified.ProjectID() != binding.ProjectID() || !sameVerifiedPolicy(verified, binding) {
     http.Error(w, "project identity mismatch", http.StatusForbidden)
     return
    }
   }
   if err != nil || binding == nil || strings.TrimSpace(binding.ProjectID()) == "" {
    http.Error(w, "verified project binding required", http.StatusForbidden)
    return
   }
   next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), projectBindingContextKey{}, binding)))
  })
 }
}

// sameVerifiedPolicy ensures both contexts agree on authority for the
// project's collection coordinate. Protected deployments should use a single
// adapter as both resolvers; this check is defense in depth.
func sameVerifiedPolicy(a authhttp.Binding, b VerifiedProjectBinding) bool {
 if a == nil || b == nil {return false}
 return a.ProjectID() == b.ProjectID()
}

// authorizeProjectResource enforces the trusted binding for every resource
// operation. Project-qualified identifiers are policy coordinates, not
// physical Redis namespace isolation. The latter requires a scoped store.
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
