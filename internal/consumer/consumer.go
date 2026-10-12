// Package consumer wires the Kafka consumer with the resilience mechanisms
// required by Phase 2 of the PRD: backpressure, circuit breaker, and graceful
// degradation (dead-letter queue).
//
// The consumer is the "processing" side of the pipeline. It reads messages
// from Kafka, processes them through a handler, and applies the resilience
// mechanisms so the pipeline survives real failure modes:
//
//   - Backpressure: when the consumer's queue backs up past a threshold,
//     intake is paused so memory/queue depth doesn't grow unbounded.
//   - Circuit breaker: when a downstream dependency fails repeatedly, the
//     breaker opens and stops calling it for a cooldown period.
//   - Graceful degradation: when the circuit is open, messages are parked in
//     a dead-letter queue instead of being crashed or silently dropped.
//
// Each of these three behaviors is independently triggerable in tests via
// the config flags or test harness.
package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	"flowguard/internal/kafka"
	"flowguard/internal/observability"
	"flowguard/internal/resilience"
)

// Handler processes a single Kafka message.
//
// Returning an error means the message could not be processed. The consumer
// will apply the circuit breaker and graceful degradation logic based on
// this error.
type Handler interface {
	// Handle processes a single Kafka message.
	// Returning nil means the message was processed successfully.
	Handle(msg *kafka.ConsumerMessage) error
}

// Config drives consumer construction.
type Config struct {
	// Kafka is the Kafka consumer configuration.
	Kafka kafka.ConsumerConfig
	// Backpressure is the backpressure configuration.
	Backpressure resilience.BackpressureConfig
	// CircuitBreakerThreshold is the number of consecutive failures
	// required to open the circuit breaker.
	CircuitBreakerThreshold int
	// CircuitBreakerCooldown is the cooldown period for the circuit breaker.
	CircuitBreakerCooldown time.Duration
	// DeadLetterTopic is the topic to send dead-letter messages to.
	// When empty, messages are stored in-memory in the dead-letter queue.
	DeadLetterTopic string
	// MaxConcurrency is the maximum number of messages processed
	// simultaneously. When > 1, the consumer fetches and queues messages
	// concurrently, which is what makes backpressure observable: the
	// in-flight queue depth grows when the producer outruns the handler.
	// When 0 or 1, processing is serial (no in-flight queue).
	MaxConcurrency int
}

// Consumer reads messages from Kafka and processes them with resilience.
type Consumer struct {
	kafka       kafka.Consumer
	handler     Handler
	breaker     *resilience.CircuitBreaker
	dlq         *resilience.DeadLetterQueue
	backpressure *resilience.BackpressureController
	metrics     *observability.Metrics

	// maxConcurrency caps the number of messages processed simultaneously.
	// Messages beyond this limit are queued in the Kafka consumer's internal
	// buffer; if that fills, the broker stops fetching (natural backpressure).
	// When set to 0 or 1, processing is serial (no in-flight queue).
	maxConcurrency int

	mu      sync.Mutex
	running bool
}

// NewConsumer creates a new consumer.
func NewConsumer(cfg Config, handler Handler) (*Consumer, error) {
	return NewConsumerWithMetrics(cfg, handler, nil)
}

// NewConsumerWithMetrics creates a new consumer with the given metrics.
// If metrics is nil, no metrics are recorded.
func NewConsumerWithMetrics(cfg Config, handler Handler, metrics *observability.Metrics) (*Consumer, error) {
	kc, err := kafka.NewConsumer(cfg.Kafka)
	if err != nil {
		return nil, err
	}

	c := &Consumer{
		kafka:          kc,
		handler:        handler,
		maxConcurrency: cfg.MaxConcurrency,
		breaker: resilience.NewCircuitBreaker(
			cfg.CircuitBreakerThreshold,
			cfg.CircuitBreakerCooldown,
		),
		dlq:     resilience.NewDeadLetterQueue(),
		metrics: metrics,
	}

	// Set up backpressure controller
	pauseFn := func() error {
		c.kafka.Pause()
		return nil
	}
	resumeFn := func() error {
		c.kafka.Resume()
		return nil
	}
	c.backpressure = resilience.NewBackpressureController(cfg.Backpressure, pauseFn, resumeFn)

	return c, nil
}

// Run starts consuming messages and processing them.
//
// This method blocks until the context is cancelled or the consumer is
// closed. It is safe to call Run concurrently with other goroutines.
//
// When MaxConcurrency > 1, messages are dispatched to a bounded pool of
// worker goroutines. The pool's buffered channel acts as the in-flight
// queue: when it fills, the consumer stops fetching from Kafka, which is
// the mechanism by which backpressure pauses intake.
func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return errors.New("consumer: already running")
	}
	c.running = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()

	concurrency := c.maxConcurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	if concurrency == 1 {
		// Serial path — no in-flight queue, backpressure depth stays near 0.
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case msg, ok := <-c.kafka.Messages():
				if !ok {
					return nil
				}
				c.processMessage(msg)
			case err := <-c.kafka.Errors():
				if err != nil {
					return err
				}
			}
		}
	}

	// Concurrent path — a bounded worker pool lets the in-flight queue
	// grow when the producer outruns the handler, which is what makes
	// backpressure observable.
	jobs := make(chan *kafka.ConsumerMessage, concurrency*2)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range jobs {
				c.processMessage(msg)
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		case msg, ok := <-c.kafka.Messages():
			if !ok {
				close(jobs)
				wg.Wait()
				return nil
			}
			jobs <- msg
		case err := <-c.kafka.Errors():
			if err != nil {
				close(jobs)
				wg.Wait()
				return err
			}
		}
	}
}

// processMessage handles a single message with the resilience mechanisms.
func (c *Consumer) processMessage(msg *kafka.ConsumerMessage) {
	start := time.Now()

	// Record the message in the backpressure controller
	c.backpressure.Record()
	defer c.backpressure.Remove()

	// Record metrics
	if c.metrics != nil {
		c.metrics.RecordConsumed(msg.Topic)
		c.metrics.SetBackpressureDepth(msg.Topic, c.backpressure.Depth())
		c.metrics.SetBackpressurePaused(msg.Topic, c.backpressure.IsPaused())
		c.metrics.SetCircuitBreakerState("default", int(c.breaker.State()))
		c.metrics.SetDeadLetterQueueDepth(msg.Topic, c.dlq.Count())
	}

	// Process the message through the circuit breaker
	err := c.breaker.Execute(func() error {
		return c.handler.Handle(msg)
	})

	if err != nil {
		// Circuit breaker is open — graceful degradation
		if errors.Is(err, resilience.ErrCircuitBreakerOpen) {
			c.dlq.Put(msg, "circuit breaker open")
			if c.metrics != nil {
				c.metrics.RecordProcessingError(msg.Topic, "circuit_breaker_open")
			}
			return
		}

		// Handler failed — park in dead-letter queue
		c.dlq.Put(msg, "handler error: "+err.Error())
		if c.metrics != nil {
			c.metrics.RecordProcessingError(msg.Topic, "handler_error")
		}
		return
	}

	// Success — record latency
	if c.metrics != nil {
		c.metrics.RecordProcessingLatency(msg.Topic, time.Since(start))
	}
}

// DeadLetterQueue returns the dead-letter queue.
func (c *Consumer) DeadLetterQueue() *resilience.DeadLetterQueue {
	return c.dlq
}

// CircuitBreakerState returns the current state of the circuit breaker.
func (c *Consumer) CircuitBreakerState() resilience.CircuitBreakerState {
	return c.breaker.State()
}

// BackpressureDepth returns the current backpressure queue depth.
func (c *Consumer) BackpressureDepth() int64 {
	return c.backpressure.Depth()
}

// BackpressureIsPaused returns whether intake is currently paused.
func (c *Consumer) BackpressureIsPaused() bool {
	return c.backpressure.IsPaused()
}

// Close shuts down the consumer and releases resources.
func (c *Consumer) Close() error {
	c.backpressure.Stop()
	return c.kafka.Close()
}

// DefaultConsumerConfig returns a sensible default configuration.
func DefaultConsumerConfig() Config {
	return Config{
		Kafka: kafka.ConsumerConfig{
			Topic:           "RECONCILIATION",
			GroupID:         "flowguard-consumers",
			ConsumerOffset:  kafka.OffsetOldest,
			ConsumerTimeout: 5 * time.Second,
		},
		Backpressure: resilience.DefaultBackpressureConfig(),
		CircuitBreakerThreshold: 3,
		CircuitBreakerCooldown:  30 * time.Second,
	}
}