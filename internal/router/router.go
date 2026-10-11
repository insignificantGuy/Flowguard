// Package router wires handlers into an http.Handler tree.
package router

import (
	"context"
	"errors"
	"net/http"
	"time"

	"flowguard/internal/config"
	"flowguard/internal/consumer"
	"flowguard/internal/handler"
	"flowguard/internal/kafka"
	"flowguard/internal/logger"
	"flowguard/internal/middleware"
	"flowguard/internal/observability"
	"flowguard/internal/resilience"
	"flowguard/internal/response"
)

// App is the assembled application: an HTTP handler plus the resources it
// owns (the Kafka producer and consumer), so the server can flush them
// during graceful shutdown.
type App struct {
	http.Handler
	producer kafka.Producer
	consumer *consumer.Consumer
	metrics  *observability.Metrics
}

// Close flushes in-flight Kafka publishes, stops the consumer, and releases
// both resources. Order matters: stop consuming before closing the producer
// so the consumer does not try to publish during shutdown.
func (a *App) Close() error {
	var firstErr error
	if a.consumer != nil {
		if err := a.consumer.Close(); err != nil {
			firstErr = err
		}
	}
	if a.producer != nil {
		if err := a.producer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// BuildWithProducer lets callers inject a custom producer — used by tests.
func BuildWithProducer(cfg config.Config, producer kafka.Producer) (*App, error) {
	return BuildWithProducerAndMetrics(cfg, producer, nil)
}

// BuildWithProducerAndMetrics lets callers inject a custom producer and metrics.
// When cfg has Kafka brokers configured, it also constructs and starts the
// resilience-aware consumer in a background goroutine so the published
// messages flow end-to-end through backpressure, circuit breaker, and the
// dead-letter queue. When brokers are not configured (tests, dev without
// Kafka), the consumer is nil and the pipeline is producer-only.
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

	app := &App{Handler: h, producer: producer, metrics: metrics}

	// Start the consumer if Kafka is configured.
	if len(cfg.KafkaBrokers) > 0 {
		cons, err := newConsumer(cfg, metrics)
		if err != nil {
			return nil, err
		}
		app.consumer = cons
		go func() {
			if err := cons.Run(context.Background()); err != nil {
				logger.Log.Error().Err(err).Msg("consumer stopped")
			}
		}()
	}

	return app, nil
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

// newConsumer builds the resilience-aware consumer from config and starts it.
//
// The consumer reads from the same topic the producer publishes to, so the
// pipeline is end-to-end: HTTP → Kafka → consumer → handler, with backpressure,
// circuit breaker, and dead-letter queue applied on the consumer side.
//
// The consumer runs in its own goroutine (started by the caller); the App
// owns it and stops it during Close().
func newConsumer(cfg config.Config, metrics *observability.Metrics) (*consumer.Consumer, error) {
	// Apply backpressure configuration with defaults
	bp := resilience.DefaultBackpressureConfig()
	if cfg.BackpressureMaxQueueDepth > 0 {
		bp.MaxQueueDepth = cfg.BackpressureMaxQueueDepth
	}
	if cfg.BackpressurePauseAfterDepth > 0 {
		bp.PauseAfterDepth = cfg.BackpressurePauseAfterDepth
	}
	if cfg.BackpressureResumeAfterDepth > 0 {
		bp.ResumeAfterDepth = cfg.BackpressureResumeAfterDepth
	}
	if cfg.BackpressureCheckInterval > 0 {
		bp.CheckInterval = time.Duration(cfg.BackpressureCheckInterval) * time.Millisecond
	}

	// Apply circuit breaker configuration with defaults
	var cbThreshold int
	if cfg.CircuitBreakerThreshold > 0 {
		cbThreshold = cfg.CircuitBreakerThreshold
	} else {
		cbThreshold = 3
	}
	var cbCooldown time.Duration
	if cfg.CircuitBreakerCooldown > 0 {
		cbCooldown = time.Duration(cfg.CircuitBreakerCooldown) * time.Millisecond
	} else {
		cbCooldown = 30 * time.Second
	}

	cons, err := consumer.NewConsumerWithMetrics(consumer.Config{
		Kafka: kafka.ConsumerConfig{
			Brokers:        cfg.KafkaBrokers,
			Topic:          cfg.KafkaTopic,
			GroupID:        "flowguard-consumers",
			ConsumerOffset: kafka.OffsetOldest,
			ConsumerTimeout: 5 * time.Second,
		},
		Backpressure:           bp,
		CircuitBreakerThreshold: cbThreshold,
		CircuitBreakerCooldown:  cbCooldown,
	}, &consumer.EchoHandler{Greeting: cfg.KafkaTopic}, metrics)
	if err != nil {
		return nil, err
	}
	return cons, nil
}

// disabledProducer is used when Kafka is not configured; every publish fails
// so the endpoint returns 503 SERVICE_UNAVAILABLE.
type disabledProducer struct{}

func (*disabledProducer) Publish(_ context.Context, _, _, _ string) error {
	return errors.New("kafka: producer is not configured")
}

func (*disabledProducer) Close() error { return nil }