// Package observability exposes Prometheus metrics for the flowthrough
// pipeline, as required by Phase 3 of the PRD.
//
// The metrics are the evidence a buyer needs to evaluate the pipeline's
// reliability claims without reading code. They are scraped by Prometheus
// and visualized in Grafana.
//
// Metrics exposed:
//   - flowguard_messages_published_total: total messages published to Kafka
//   - flowguard_messages_consumed_total: total messages consumed from Kafka
//   - flowguard_processing_latency_seconds: processing latency histogram
//   - flowguard_processing_errors_total: total processing errors
//   - flowguard_circuit_breaker_state: current state of the circuit breaker
//   - flowguard_dead_letter_queue_depth: current depth of the dead-letter queue
//   - flowguard_backpressure_depth: current backpressure queue depth
//   - flowguard_backpressure_paused: whether intake is currently paused
package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the Prometheus collectors for the pipeline.
type Metrics struct {
	gatherer                 prometheus.Gatherer
	MessagesPublishedTotal   *prometheus.CounterVec
	MessagesConsumedTotal    *prometheus.CounterVec
	ProcessingLatencySeconds *prometheus.HistogramVec
	ProcessingErrorsTotal    *prometheus.CounterVec
	CircuitBreakerState      *prometheus.GaugeVec
	DeadLetterQueueDepth     *prometheus.GaugeVec
	BackpressureDepth        *prometheus.GaugeVec
	BackpressurePaused       *prometheus.GaugeVec
}

// newMetrics creates a Metrics instance with all collectors registered.
func newMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{gatherer: reg,
		MessagesPublishedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "flowguard_messages_published_total",
				Help: "Total messages published to Kafka.",
			},
			[]string{"topic"},
		),
		MessagesConsumedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "flowguard_messages_consumed_total",
				Help: "Total messages consumed from Kafka.",
			},
			[]string{"topic"},
		),
		ProcessingLatencySeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "flowguard_processing_latency_seconds",
				Help:    "Latency of message processing in seconds.",
				Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
			[]string{"topic"},
		),
		ProcessingErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "flowguard_processing_errors_total",
				Help: "Total errors encountered while processing messages.",
			},
			[]string{"topic", "reason"},
		),
		CircuitBreakerState: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "flowguard_circuit_breaker_state",
				Help: "Current state of the circuit breaker (0=closed, 1=open, 2=half-open).",
			},
			[]string{"name"},
		),
		DeadLetterQueueDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "flowguard_dead_letter_queue_depth",
				Help: "Current depth of the dead-letter queue.",
			},
			[]string{"topic"},
		),
		BackpressureDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "flowguard_backpressure_depth",
				Help: "Current depth of the backpressure queue.",
			},
			[]string{"topic"},
		),
		BackpressurePaused: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "flowguard_backpressure_paused",
				Help: "Whether intake is currently paused (1=paused, 0=not paused).",
			},
			[]string{"topic"},
		),
	}

	// Register all collectors with the given registry
	reg.MustRegister(m.MessagesPublishedTotal)
	reg.MustRegister(m.MessagesConsumedTotal)
	reg.MustRegister(m.ProcessingLatencySeconds)
	reg.MustRegister(m.ProcessingErrorsTotal)
	reg.MustRegister(m.CircuitBreakerState)
	reg.MustRegister(m.DeadLetterQueueDepth)
	reg.MustRegister(m.BackpressureDepth)
	reg.MustRegister(m.BackpressurePaused)

	return m
}

// NewMetrics owns its registry so independent applications never double-register.
func NewMetrics() *Metrics {
	return newMetrics(prometheus.NewRegistry())
}

// NewTestMetrics creates isolated collectors for tests.
func NewTestMetrics() *Metrics {
	return newMetrics(prometheus.NewRegistry())
}

// RecordPublished records a published message.
func (m *Metrics) RecordPublished(topic string) {
	m.MessagesPublishedTotal.WithLabelValues(topic).Inc()
}

// RecordConsumed records a consumed message.
func (m *Metrics) RecordConsumed(topic string) {
	m.MessagesConsumedTotal.WithLabelValues(topic).Inc()
}

// RecordProcessingLatency records the latency of processing a message.
func (m *Metrics) RecordProcessingLatency(topic string, latency time.Duration) {
	m.ProcessingLatencySeconds.WithLabelValues(topic).Observe(latency.Seconds())
}

// RecordProcessingError records a processing error.
func (m *Metrics) RecordProcessingError(topic, reason string) {
	m.ProcessingErrorsTotal.WithLabelValues(topic, reason).Inc()
}

// SetCircuitBreakerState sets the circuit breaker state gauge.
func (m *Metrics) SetCircuitBreakerState(name string, state int) {
	m.CircuitBreakerState.WithLabelValues(name).Set(float64(state))
}

// SetDeadLetterQueueDepth sets the dead-letter queue depth gauge.
func (m *Metrics) SetDeadLetterQueueDepth(topic string, depth int) {
	m.DeadLetterQueueDepth.WithLabelValues(topic).Set(float64(depth))
}

// SetBackpressureDepth sets the backpressure depth gauge.
func (m *Metrics) SetBackpressureDepth(topic string, depth int64) {
	m.BackpressureDepth.WithLabelValues(topic).Set(float64(depth))
}

// SetBackpressurePaused sets the backpressure paused gauge.
func (m *Metrics) SetBackpressurePaused(topic string, paused bool) {
	if paused {
		m.BackpressurePaused.WithLabelValues(topic).Set(1)
	} else {
		m.BackpressurePaused.WithLabelValues(topic).Set(0)
	}
}

// Handler returns an HTTP handler that serves Prometheus metrics at /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.gatherer, promhttp.HandlerOpts{})
}
