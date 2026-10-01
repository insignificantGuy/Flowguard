// Package model defines the API's request and response shapes.
//
// These are the only cross-cutting types; everything else lives in its module.
package model

import "time"

// SendMessageRequest is the body of POST /v1/send-message.
type SendMessageRequest struct {
	Message string `json:"message"`
}

// SendMessageResponse is returned by POST /v1/send-message.
type SendMessageResponse struct {
	Echo       string    `json:"echo"`
	ReceivedAt time.Time `json:"receivedAt"`
}

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	Status       string  `json:"status"`
	Version      string  `json:"version"`
	UptimeSeconds float64 `json:"uptimeSeconds"`
	NodeEnv      string  `json:"nodeEnv"`
}

// APIError is the standard error envelope.
type APIError struct {
	Error struct {
		Code     string      `json:"code"`
		Message  string      `json:"message"`
		TraceID  string      `json:"traceId"`
		Details  any `json:"details,omitempty"`
	} `json:"error"`
}

// APIResponse is the standard success envelope.
type APIResponse struct {
	Data      any `json:"data"`
	TraceID   string      `json:"traceId"`
	Timestamp string      `json:"timestamp"`
}