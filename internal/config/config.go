// Package config loads and validates process configuration.
//
// A misconfigured deployment must fail fast with a clear message rather than
// crashing later at first use. Values are read from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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

	// KafkaBrokers is the list of Kafka broker addresses (comma-separated).
	// When empty, the producer is disabled and /v1/send-message returns 503.
	KafkaBrokers []string
	// KafkaTopic is the topic the send-message endpoint publishes to.
	KafkaTopic string
	// KafkaTimeout is how long Publish waits for a broker ack.
	KafkaTimeout time.Duration

	// BackpressureMaxQueueDepth is the maximum queue depth before backpressure kicks in.
	// 0 means use default (1000).
	BackpressureMaxQueueDepth int
	// BackpressurePauseAfterDepth triggers a pause when queue depth reaches this value.
	// 0 means use default (800).
	BackpressurePauseAfterDepth int
	// BackpressureResumeAfterDepth triggers a resume when queue depth drops below this value.
	// 0 means use default (200).
	BackpressureResumeAfterDepth int
	// BackpressureCheckInterval is how often the backpressure controller checks the queue depth.
	// 0 means use default (100ms).
	BackpressureCheckInterval int

	// CircuitBreakerThreshold is the number of consecutive failures to open the circuit breaker.
	// 0 means use default (3).
	CircuitBreakerThreshold int
	// CircuitBreakerCooldown is the cooldown period after which the circuit breaker allows a trial call.
	// 0 means use default (30s).
	CircuitBreakerCooldown int
	// ConsumerMaxConcurrency is the number of messages processed simultaneously.
	// When 0, defaults to 4. Higher values allow more in-flight messages,
	// which makes backpressure more likely to trigger under load.
	ConsumerMaxConcurrency int
	// HandlerProcessingMs is the simulated processing time per message.
	// When 0, defaults to 5ms. Higher values slow the handler relative to
	// the producer, which is what causes the in-flight queue to back up.
	HandlerProcessingMs int
}

// Load reads configuration from the environment with sane defaults.
func Load() (Config, error) {
	c := Config{
		Env:             getEnv("ENV", "development"),
		Host:            getEnv("HOST", "0.0.0.0"),
		Port:            getEnvInt("PORT", 3000),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		RateLimitMax:    getEnvInt("RATE_LIMIT_MAX", 100),
		RateLimitWindow: time.Duration(getEnvInt("RATE_LIMIT_TTL_MS", 60000)) * time.Millisecond,
		RedisURL:        osEnv("REDIS_URL"),
		KafkaBrokers:    splitCSV(osEnv("KAFKA_BROKERS")),
		KafkaTopic:      getEnv("KAFKA_TOPIC", "RECONCILIATION"),
		KafkaTimeout:    time.Duration(getEnvInt("KAFKA_TIMEOUT_MS", 5000)) * time.Millisecond,
		BackpressureMaxQueueDepth:    getEnvInt("BACKPRESSURE_MAX_QUEUE_DEPTH", 0),
		BackpressurePauseAfterDepth:  getEnvInt("BACKPRESSURE_PAUSE_AFTER_DEPTH", 0),
		BackpressureResumeAfterDepth: getEnvInt("BACKPRESSURE_RESUME_AFTER_DEPTH", 0),
		BackpressureCheckInterval:    getEnvInt("BACKPRESSURE_CHECK_INTERVAL_MS", 0),
		CircuitBreakerThreshold:      getEnvInt("CIRCUIT_BREAKER_THRESHOLD", 0),
		CircuitBreakerCooldown:       getEnvInt("CIRCUIT_BREAKER_COOLDOWN_MS", 0),
		ConsumerMaxConcurrency:       getEnvInt("CONSUMER_MAX_CONCURRENCY", 0),
		HandlerProcessingMs:          getEnvInt("HANDLER_PROCESSING_MS", 0),
	}

	if c.Port < 1 || c.Port > 65535 {
		return Config{}, fmt.Errorf("PORT must be between 1 and 65535, got %d", c.Port)
	}
	if c.RateLimitMax < 1 {
		return Config{}, fmt.Errorf("RATE_LIMIT_MAX must be >= 1, got %d", c.RateLimitMax)
	}
	if c.BackpressureMaxQueueDepth < 0 {
		return Config{}, fmt.Errorf("BACKPRESSURE_MAX_QUEUE_DEPTH must be >= 0, got %d", c.BackpressureMaxQueueDepth)
	}
	if c.BackpressurePauseAfterDepth < 0 {
		return Config{}, fmt.Errorf("BACKPRESSURE_PAUSE_AFTER_DEPTH must be >= 0, got %d", c.BackpressurePauseAfterDepth)
	}
	if c.BackpressureResumeAfterDepth < 0 {
		return Config{}, fmt.Errorf("BACKPRESSURE_RESUME_AFTER_DEPTH must be >= 0, got %d", c.BackpressureResumeAfterDepth)
	}
	if c.BackpressureCheckInterval < 0 {
		return Config{}, fmt.Errorf("BACKPRESSURE_CHECK_INTERVAL_MS must be >= 0, got %d", c.BackpressureCheckInterval)
	}
	if c.CircuitBreakerThreshold < 0 {
		return Config{}, fmt.Errorf("CIRCUIT_BREAKER_THRESHOLD must be >= 0, got %d", c.CircuitBreakerThreshold)
	}
	if c.CircuitBreakerCooldown < 0 {
		return Config{}, fmt.Errorf("CIRCUIT_BREAKER_COOLDOWN_MS must be >= 0, got %d", c.CircuitBreakerCooldown)
	}
	if c.ConsumerMaxConcurrency < 0 {
		return Config{}, fmt.Errorf("CONSUMER_MAX_CONCURRENCY must be >= 0, got %d", c.ConsumerMaxConcurrency)
	}
	if c.HandlerProcessingMs < 0 {
		return Config{}, fmt.Errorf("HANDLER_PROCESSING_MS must be >= 0, got %d", c.HandlerProcessingMs)
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
	if c.KafkaTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_TOPIC must not be empty")
	}
	return c, nil
}

// splitCSV parses a comma-separated list of brokers, tolerating surrounding
// whitespace and empty entries.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' })
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
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
