// Package serverless exposes Logma's reusable serverless HTTP runtime as a
// first-class subpackage of the main Logma module.
package serverless

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/xd-dash/logma/serverless/router"
)

// Build constructs the standard serverless router shell and lets an embedding
// application register its own routes.
func Build(register func(r chi.Router)) http.Handler {
	return router.Build(register)
}

// NewRouter returns Logma's standalone Redis Pub/Sub SSE service.
func NewRouter() http.Handler {
	return router.NewRouter()
}
