// Package response provides helpers for writing the standard API envelope.
//
// Every successful response looks like:
//
//	{ "data": ..., "traceId": "...", "timestamp": "..." }
//
// Consistency at the boundary is what makes clients easy to write against.
package response

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// TraceIDHeader is the inbound/outbound header used for end-to-end tracing.
const TraceIDHeader = "X-Request-Id"

// Send writes a successful payload in the standard envelope.
func Send(w http.ResponseWriter, r *http.Request, status int, data any) {
	traceID := TraceID(r)
	w.Header().Set(TraceIDHeader, traceID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Data      any     `json:"data"`
		TraceID   string  `json:"traceId"`
		Timestamp string  `json:"timestamp"`
	}{
		Data:      data,
		TraceID:   traceID,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
}

// TraceID returns the inbound request id or generates one.
func TraceID(r *http.Request) string {
	if id := r.Header.Get(TraceIDHeader); id != "" {
		return id
	}
	return uuid.NewString()
}

// Error writes a standard error envelope.
func Error(w http.ResponseWriter, r *http.Request, status int, code, message string, details any) {
	traceID := TraceID(r)
	w.Header().Set(TraceIDHeader, traceID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error struct {
			Code     string `json:"code"`
			Message  string `json:"message"`
			TraceID  string `json:"traceId"`
			Details  any    `json:"details,omitempty"`
		} `json:"error"`
	}{
		Error: struct {
			Code     string `json:"code"`
			Message  string `json:"message"`
			TraceID  string `json:"traceId"`
			Details  any    `json:"details,omitempty"`
		}{
			Code:     code,
			Message:  message,
			TraceID:  traceID,
			Details:  details,
		},
	})
}