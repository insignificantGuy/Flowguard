// Package router wires handlers into an http.Handler tree.
package router

import (
	"context"
	"errors"
	"net/http"

	"flowguard/internal/config"
	"flowguard/internal/handler"
	"flowguard/internal/kafka"
	"flowguard/internal/middleware"
	"flowguard/internal/observability"
	"flowguard/internal/response"
)

// App is the assembled application: an HTTP handler plus the resources it
// owns (currently the Kafka producer), so the server can flush them during
// graceful shutdown.
type App struct {
	http.Handler
	producer kafka.Producer
	consumer *kafka.Consumer // deprecated: use consumer.Consumer instead
	metrics  *observability.Metrics
}

// Close flushes in-flight Kafka publishes and releases the producer.
func (a *App) Close() error {
	if a.producer == nil {
		return nil
	}
	return a.producer.Close()
}

// BuildWithProducer lets callers inject a custom producer — used by tests.
func BuildWithProducer(cfg config.Config, producer kafka.Producer) (*App, error) {
	return BuildWithProducerAndMetrics(cfg, producer, nil)
}

// BuildWithProducerAndMetrics lets callers inject a custom producer and metrics.
func BuildWithProducerAndMetrics(cfg config.Config, producer kafka.Producer, metrics *observability.Metrics) (*App, error) {
	if metrics == nil {
		metrics = observability.NewMetrics()
	}

	mux := http.NewServeMux()
	mux.Handle("/health", handler.NewHealth("1.0.0", cfg.Env))
	mux.Handle("/openapi.json", handler.NewOpenAPI())
	mux.Handle("/v1/send-message", handler.NewSendMessage("Hello", cfg.KafkaTopic, producer, metrics))
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/{path...}", func(w http.ResponseWriter, r *http.Request) {
		response.Error(w, r, http.StatusNotFound, "NOT_FOUND", "Route not found", nil)
	})

	h := middleware.Recovery(mux)
	h = middleware.Security(h)
	h = middleware.Logger(h)
	h = middleware.RateLimit(cfg)(h)

	return &App{Handler: h, producer: producer, metrics: metrics}, nil
}

// Build constructs the complete HTTP handler with all middleware and routes.
//
// Middleware order matters (outer to inner):
//
//	Recovery → Security → Logger → RateLimit → Routes → 404
func Build(cfg config.Config) (*App, error) {
	producer, err := newProducer(cfg)
	if err != nil {
		return nil, err
	}
	return BuildWithProducer(cfg, producer)
}

// newProducer builds the Kafka producer from config.
//
// When brokers are not configured, a disabled producer is returned so the
// endpoint degrades to 503 instead of crashing the whole process — useful for
// local development and tests that don't need a live Kafka.
func newProducer(cfg config.Config) (kafka.Producer, error) {
	if len(cfg.KafkaBrokers) == 0 {
		return &disabledProducer{}, nil
	}
	return kafka.New(kafka.Config{
		Brokers: cfg.KafkaBrokers,
		Timeout: cfg.KafkaTimeout,
	})
}

// disabledProducer is used when Kafka is not configured; every publish fails
// so the endpoint returns 503 SERVICE_UNAVAILABLE.
type disabledProducer struct{}

func (*disabledProducer) Publish(_ context.Context, _, _, _ string) error {
	return errors.New("kafka: producer is not configured")
}

func (*disabledProducer) Close() error { return nil }