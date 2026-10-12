// Package consumer provides the message handler implementations.
//
// The handler is the "business logic" side of the pipeline: it receives a
// decoded Kafka message and does something with it. Keeping the handler in
// its own package lets the consumer stay decoupled from the HTTP layer and
// trivially testable with the stub consumer in kafka.NewStubConsumer.
package consumer

import (
	"fmt"
	"time"

	"flowguard/internal/kafka"
)

// EchoHandler is a trivial handler that echoes the message value back into
// the log. It is the default handler used by the production server and is
// intentionally simple so the resilience mechanisms (backpressure, circuit
// breaker, dead-letter queue) are the thing under test, not business logic.
//
// Returning an error from Handle triggers the circuit breaker and, if the
// breaker is open, graceful degradation via the dead-letter queue.
type EchoHandler struct {
	// Greeting is prepended to the echoed message. Empty defaults to "echo".
	Greeting string
	// ProcessingTime simulates the work a real handler would do. It is
	// configurable so backpressure can be demonstrated under load: with a
	// slow handler and a fast producer, the in-flight queue depth grows past
	// the pause threshold and intake is paused.
	ProcessingTime time.Duration
	// FailureRate is the probability (0.0-1.0) that Handle returns an error.
	// Used to demonstrate the circuit breaker and dead-letter queue.
	FailureRate float64
}

// Handle implements consumer.Handler.
func (h *EchoHandler) Handle(msg *kafka.ConsumerMessage) error {
	greeting := h.Greeting
	if greeting == "" {
		greeting = "echo"
	}
	fmt.Printf("%s: consumed message topic=%s partition=%d offset=%d key=%q value=%q\n",
		greeting, msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)

	if h.ProcessingTime > 0 {
		time.Sleep(h.ProcessingTime)
	}

	// Random failure injection for circuit breaker demonstration
	if h.FailureRate > 0 {
		// Use a deterministic pseudo-random based on message offset so the
		// same message always fails/succeeds (replayable).
		if msg.Offset%100 < int64(h.FailureRate*100) {
			return fmt.Errorf("simulated handler failure for offset %d", msg.Offset)
		}
	}

	return nil
}