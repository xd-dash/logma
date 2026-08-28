// Package router exposes the reusable HTTP shell from logma-serverless through
// Logma's canonical module path.
package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	implementation "github.com/xd-dash/logma-serverless/router"
)

// Build constructs the shared serverless router shell and lets register mount
// application-specific routes.
func Build(register func(r chi.Router)) http.Handler {
	return implementation.Build(register)
}

// NewRouter returns the standalone logma-serverless deployment router. Most
// embedding applications should call Build with their own routes instead.
func NewRouter() http.Handler {
	return implementation.NewRouter()
}
