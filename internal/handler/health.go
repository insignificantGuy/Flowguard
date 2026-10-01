package handler

import (
	"net/http"
	"time"

	"flowguard/internal/model"
	"flowguard/internal/response"
)

// Health handles GET /health — a liveness probe.
//
// A readiness probe would additionally check dependencies (DB, queue) here.
type Health struct {
	started  time.Time
	version  string
	nodeEnv  string
}

// NewHealth creates a Health handler.
func NewHealth(version, nodeEnv string) *Health {
	return &Health{started: time.Now(), version: version, nodeEnv: nodeEnv}
}

// ServeHTTP implements http.Handler.
func (h *Health) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		response.Error(w, r, http.StatusMethodNotAllowed,
			"METHOD_NOT_ALLOWED", "Use GET", nil)
		return
	}
	response.Send(w, r, http.StatusOK, model.HealthResponse{
		Status:        "ok",
		Version:       h.version,
		UptimeSeconds: time.Since(h.started).Seconds(),
		NodeEnv:       h.nodeEnv,
	})
}