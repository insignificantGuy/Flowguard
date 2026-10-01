// Package middleware holds HTTP middleware.
package middleware

import (
	"net/http"
	"runtime/debug"
	"time"

	"github.com/rs/zerolog"

	"flowguard/internal/logger"
	"flowguard/internal/response"
)

// Logger is a structured HTTP request logger.
//
// Logs one line per request with status, latency, and trace id. 5xx are
// logged as errors and 4xx as warnings so they stand out in a dashboard.
func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := &wrapWriter{ResponseWriter: w, status: 200}
		start := time.Now()
		trace := response.TraceID(r)

		logger.Log.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Str("remote", r.RemoteAddr).
			Str("trace", trace).
			Msg("incoming request")

		next.ServeHTTP(ww, r)

		lv := zerolog.InfoLevel
		if ww.status >= 500 {
			lv = zerolog.ErrorLevel
		} else if ww.status >= 400 {
			lv = zerolog.WarnLevel
		}
		logger.Log.WithLevel(lv).
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", ww.status).
			Dur("latency", time.Since(start)).
			Msg("outgoing response")
	})
}

// Recovery recovers from panics in handlers so a single bad request cannot
// crash the process. This is essential for horizontal scaling: a worker that
// crashes takes down its in-flight requests and forces a restart.
func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				logger.Log.Error().
					Str("trace", response.TraceID(r)).
					Any("panic", rec).
					Str("stack", string(debug.Stack())).
					Msg("recovered from panic")
				response.Error(w, r, http.StatusInternalServerError,
					"INTERNAL_ERROR", "Internal server error", nil)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// Security sets response headers that reduce attack surface.
func Security(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		w.Header().Set("X-Powered-By", "") // never advertise the framework
		next.ServeHTTP(w, r)
	})
}

// wrapWriter records the status code so the logger can report it.
type wrapWriter struct {
	http.ResponseWriter
	status int
}

// WriteHeader records the status code (default 200).
func (w *wrapWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Write is overridden so the logger can observe the payload; the embedded
// ResponseWriter handles the actual bytes.
func (w *wrapWriter) Write(b []byte) (int, error) {
	return w.ResponseWriter.Write(b)
}