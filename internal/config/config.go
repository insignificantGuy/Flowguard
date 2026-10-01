// Package config loads and validates process configuration.
//
// A misconfigured deployment must fail fast with a clear message rather than
// crashing later at first use. Values are read from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds every tunable in the service.
type Config struct {
	// Env is the deployment environment (development | staging | production).
	Env string

	// Host and Port the HTTP server binds to.
	Host string
	Port int

	// LogLevel controls zerolog verbosity.
	LogLevel string

	// RateLimitMax is the number of requests allowed per key per window.
	RateLimitMax int
	// RateLimitWindow is the rolling window for the rate limiter.
	RateLimitWindow time.Duration

	// RedisURL, when set, makes the rate limiter shared across instances.
	// Empty means an in-memory limiter (single-process only).
	RedisURL string
}

// Load reads configuration from the environment with sane defaults.
func Load() (Config, error) {
	c := Config{
		Env:             getEnv("ENV", "development"),
		Host:            getEnv("HOST", "0.0.0.0"),
		Port:            getEnvInt("PORT", 3000),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		RateLimitMax:     getEnvInt("RATE_LIMIT_MAX", 100),
		RateLimitWindow:  time.Duration(getEnvInt("RATE_LIMIT_TTL_MS", 60000)) * time.Millisecond,
		RedisURL:        osEnv("REDIS_URL"),
	}

	if c.Port < 1 || c.Port > 65535 {
		return Config{}, fmt.Errorf("PORT must be between 1 and 65535, got %d", c.Port)
	}
	if c.RateLimitMax < 1 {
		return Config{}, fmt.Errorf("RATE_LIMIT_MAX must be >= 1, got %d", c.RateLimitMax)
	}
	switch c.Env {
	case "development", "staging", "production":
	case "":
		c.Env = "development"
	default:
		return Config{}, fmt.Errorf("unknown ENV %q (expected development|staging|production)", c.Env)
	}
	switch c.LogLevel {
	case "trace", "debug", "info", "warn", "error", "fatal":
	default:
		return Config{}, fmt.Errorf("unknown LOG_LEVEL %q", c.LogLevel)
	}
	return c, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func osEnv(key string) string { return os.Getenv(key) }

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}