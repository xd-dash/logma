// Package prajapati adapts Prajapati's verified project binding to the
// shared Logma/Skymill HTTP authentication contract.
package prajapati

import (
 "context"
 "errors"

 "github.com/xd-dash/logma/serverless/authhttp"
 "github.com/xd-dash/prajapati/binding"
)

// Resolver delegates authentication and exact policy binding to Prajapati.
// Verifier must be a production credential verifier, not a caller-controlled
// principal lookup. Bindings must come from trusted deployment configuration.
type Resolver struct {
 Verifier binding.Verifier
 Bindings []binding.ProjectBinding
}

// Compile verifies the principal and digest-pinned project policy on every
// request. An absent verifier or binding fails closed.
func (r Resolver) Resolve(ctx context.Context, credential, audience string) (authhttp.Binding,error) {
 if r.Verifier == nil || len(r.Bindings)==0 {return nil,errors.New("prajapati authority unavailable")}
 return binding.Resolve(ctx,r.Verifier,credential,audience,r.Bindings)
}
