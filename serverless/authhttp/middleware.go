// Package authhttp provides composable HTTP bearer authentication for
// application-owned routers. Credential verification and policy authority are
// supplied by a trusted resolver, not inferred from caller-controlled headers.
//
// A handler may be hosted directly or composed as Gospace's native http.Handler.
// The package does not implement a router, register routes or manage processes.
package authhttp

import (
 "context"
 "errors"
 "net/http"
 "strings"
)

// Binding is an authenticated, policy-bound identity. The implementation
// remains owned by the authority provider (for example Prajapati).
type Binding interface {
 ProjectID() string
 Authorize(action, resource string) error
}

// Resolver authenticates the bearer credential and returns a policy-bound
// identity for the configured service audience. Implementations MUST validate
// signature, issuer, expiry, exact audience, policy digest and revocation.
type Resolver interface {
 Resolve(context.Context, string, string) (Binding, error)
}

type identityKey struct{}

// Authenticate adapts the middleware to services whose existing HTTP API
// accepts a request-to-context authentication callback (for example Skymill).
// The supplied callback receives the request with its verified binding in
// context and must derive service-specific grants; this function never grants
// stream or resource permissions itself.
func Authenticate(audience string, resolver Resolver, grant func(*http.Request, Binding) (context.Context, error)) func(*http.Request) (context.Context, error) {
 return func(r *http.Request) (context.Context, error) {
  if grant == nil { return nil, errors.New("grant mapper required") }
  credential, err := bearer(r)
  if err != nil || resolver == nil || audience == "" {return nil, errors.New("bearer credential required")}
  binding, err := resolver.Resolve(r.Context(), credential, audience)
  if err != nil || binding == nil || binding.ProjectID() == "" {return nil, errors.New("verified binding required")}
  return grant(r.WithContext(context.WithValue(r.Context(), identityKey{}, binding)), binding)
 }
}


// ContextResolver adapts a verified HTTP context into application-owned
// project resolver interfaces without authenticating the request twice.
type ContextResolver struct{}

func (ContextResolver) ResolveProjectBinding(ctx context.Context) (Binding, error) {
 binding, ok := FromContext(ctx)
 if !ok || binding.ProjectID() == "" {return nil, errors.New("verified HTTP binding required")}
 return binding, nil
}


// FromContext returns only a binding installed by this middleware.
func FromContext(ctx context.Context) (Binding, bool) {
 binding, ok := ctx.Value(identityKey{}).(Binding)
 return binding, ok && binding != nil
}

func bearer(r *http.Request) (string, error) {
 values, exists := r.Header["Authorization"]
 if !exists || len(values) != 1 { return "", errors.New("exactly one Authorization header required") }
 parts := strings.SplitN(values[0], " ", 2)
 if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" || strings.TrimSpace(parts[1]) != parts[1] || strings.ContainsAny(parts[1], " \t\r\n,") {
  return "", errors.New("invalid bearer credential")
 }
 return parts[1], nil
}

// Middleware authenticates each request and installs its verified binding in
// context. Nil resolvers or empty audiences reject all requests (fail closed).
// Service handlers must still authorize the exact action/resource; this layer
// does not substitute for Logma graph ACLs or Skymill stream grants.
func Middleware(audience string, resolver Resolver) func(http.Handler) http.Handler {
 return func(next http.Handler) http.Handler {
  return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
   if resolver == nil || audience == "" || next == nil {
    http.Error(w, "authentication unavailable", http.StatusServiceUnavailable)
    return
   }
   credential, err := bearer(r)
   if err != nil { http.Error(w, "bearer credential required", http.StatusUnauthorized); return }
   binding, err := resolver.Resolve(r.Context(), credential, audience)
   if err != nil || binding == nil || binding.ProjectID() == "" {
    http.Error(w, "invalid credential or binding", http.StatusUnauthorized)
    return
   }
   next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), identityKey{}, binding)))
  })
 }
}
