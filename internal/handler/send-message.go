// Package handler exposes HTTP handlers for the API.
package handler

import (
	"encoding/json"
	"net/http"

	"flowguard/internal/kafka"
	"flowguard/internal/model"
	"flowguard/internal/observability"
	"flowguard/internal/response"
	"flowguard/internal/service"
)

// SendMessage handles POST /v1/send-message.
//
// It validates the request body, delegates to the service, and writes the
// standard response envelope. Validation failures map to a stable
// VALIDATION_ERROR code so clients can surface the right input error. A Kafka
// publish failure maps to 503 SERVICE_UNAVAILABLE.
type SendMessage struct {
	svc     *service.SendMessageService
	metrics *observability.Metrics
}

// NewSendMessage creates a SendMessage handler.
func NewSendMessage(greeting, topic string, producer kafka.Producer, metrics *observability.Metrics) *SendMessage {
	return &SendMessage{
		svc:     service.NewSendMessageService(greeting, topic, producer),
		metrics: metrics,
	}
}

// ServeHTTP implements http.Handler.
func (h *SendMessage) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		response.Error(w, r, http.StatusMethodNotAllowed,
			"METHOD_NOT_ALLOWED", "Use POST", nil)
		return
	}

	var req model.SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.Error(w, r, http.StatusBadRequest,
			"VALIDATION_ERROR", "Invalid request body", nil)
		return
	}
	if req.Message == "" {
		response.Error(w, r, http.StatusBadRequest,
			"VALIDATION_ERROR", "message must not be empty", nil)
		return
	}

	result, err := h.svc.Send(r.Context(), req)
	if err != nil {
		// Publish failures are transient (broker down, timeouts). Surface them
		// as 503 so the client can retry, but never leak the underlying error.
		response.Error(w, r, http.StatusServiceUnavailable,
			"SERVICE_UNAVAILABLE", "Failed to publish message", nil)
		return
	}

	// Record metrics on successful publish
	if h.metrics != nil {
		h.metrics.RecordPublished(req.Message)
	}

	response.Send(w, r, http.StatusOK, result)
}