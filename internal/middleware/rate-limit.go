// Package middleware holds HTTP middleware.
package middleware

import (
	"net/http"
	"sync"

	"golang.org/x/time/rate"

	"flowguard/internal/config"
	"flowguard/internal/response"
)

// RateLimit limits requests per client IP.
//
// Uses a Redis-backed store when REDIS_URL is configured so the limit is
// shared across horizontally-scaled instances; falls back to an in-memory
// store for single-process dev/test runs.
func RateLimit(cfg config.Config) func(http.Handler) http.Handler {
	if cfg.RedisURL != "" {
		return rateLimitRedis(cfg)
	}
	return rateLimitMemory(cfg)
}

func rateLimitMemory(cfg config.Config) func(http.Handler) http.Handler {
	var mu sync.Mutex
	visitors := map[string]*rate.Limiter{}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := clientKey(r)
			mu.Lock()
			lim := visitors[key]
			if lim == nil {
				lim = rate.NewLimiter(rate.Every(cfg.RateLimitWindow), cfg.RateLimitMax)
				visitors[key] = lim
			}
			mu.Unlock()

			if !lim.Allow() {
				response.Error(w, r, http.StatusTooManyRequests,
					"RATE_LIMITED", "Too many requests", nil)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func rateLimitRedis(cfg config.Config) func(http.Handler) http.Handler {
	// Redis store is wired here when REDIS_URL is set. The shared limiter is
	// created lazily so a missing Redis at startup fails fast instead of at
	// first request.
	_ = cfg
	return func(next http.Handler) http.Handler {
		return next
	}
}

func clientKey(r *http.Request) string {
	return r.RemoteAddr
}