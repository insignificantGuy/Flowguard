// Package router wires handlers into an http.Handler tree.
package router

import (
	"net/http"

	"flowguard/internal/config"
	"flowguard/internal/handler"
	"flowguard/internal/middleware"
	"flowguard/internal/response"
)

// Build constructs the complete HTTP handler with all middleware and routes.
//
// Middleware order matters (outer to inner):
//   Recovery → Security → Logger → RateLimit → Routes → 404
func Build(cfg config.Config) http.Handler {
	mux := http.NewServeMux()

	// --- Routes ------------------------------------------------------------
	mux.Handle("/health", handler.NewHealth("1.0.0", cfg.Env))
	mux.Handle("/openapi.json", handler.NewOpenAPI())
	mux.Handle("/v1/send-message", handler.NewSendMessage("Hello"))

	// --- 404 fallback ------------------------------------------------------
	// `/{path...}` is the catch-all: the mux's longest-prefix matching means
	// registered routes win, everything else falls through here.
	mux.HandleFunc("/{path...}", func(w http.ResponseWriter, r *http.Request) {
		response.Error(w, r, http.StatusNotFound,
			"NOT_FOUND", "Route not found", nil)
	})

	// --- Middleware (outer to inner) ---------------------------------------
	// Recovery wraps everything so panics anywhere are caught.
	h := middleware.Recovery(mux)
	h = middleware.Security(h)
	h = middleware.Logger(h)
	h = middleware.RateLimit(cfg)(h)

	return h
}