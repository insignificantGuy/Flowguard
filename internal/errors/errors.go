// Package errors defines the domain error hierarchy.
//
// Every error carries a stable `Code` that clients (and our own services) can
// switch on. AppError maps to HTTP responses; everything else is internal and
// gets logged + returned as a generic 500.
package errors

import "net/http"

// AppError is a known, client-safe error with a stable code.
type AppError struct {
	Message    string
	Code       string
	StatusCode int
	Details    any
}

func (e *AppError) Error() string { return e.Message }

// New creates an AppError.
func New(message, code string, status int, details any) *AppError {
	return &AppError{Message: message, Code: code, StatusCode: status, Details: details}
}

// Predefined constructors.
var (
	ValidationError = func(message string, details any) *AppError {
		return New(message, "VALIDATION_ERROR", http.StatusBadRequest, details)
	}
	NotFoundError = func(resource string) *AppError {
		return New(resource+" not found", "NOT_FOUND", http.StatusNotFound, nil)
	}
	RateLimitError = func(message ...string) *AppError {
		m := "Too many requests"
		if len(message) > 0 && message[0] != "" {
			m = message[0]
		}
		return New(m, "RATE_LIMITED", http.StatusTooManyRequests, nil)
	}
	InternalError = func(message string, details any) *AppError {
		return New(message, "INTERNAL_ERROR", http.StatusInternalServerError, details)
	}
)