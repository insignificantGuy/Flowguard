package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"flowguard/internal/kafka"
	"flowguard/internal/resilience"
)

// stubHandler implements Handler for testing.
type stubHandler struct {
	err error
}

func (h *stubHandler) Handle(msg *kafka.ConsumerMessage) error {
	return h.err
}

func TestConsumer_Success(t *testing.T) {
	cfg := DefaultConsumerConfig()
	cfg.Kafka.Brokers = []string{"dummy"}
	cfg.Kafka.Topic = "test-topic"
	cfg.Kafka.GroupID = "test-group"
	cfg.Backpressure = resilience.BackpressureConfig{
		MaxQueueDepth:    10,
		PauseAfterDepth:  5,
		ResumeAfterDepth: 2,
		CheckInterval:    1 * time.Millisecond,
	}
	cfg.CircuitBreakerThreshold = 3
	cfg.CircuitBreakerCooldown = 10 * time.Millisecond

	// Use a stub consumer since we don't have a real Kafka
	stubKafka := kafka.NewStubConsumer("test-topic")
	handler := &stubHandler{err: nil}

	c := &Consumer{
		kafka:   stubKafka,
		handler: handler,
		breaker: resilience.NewCircuitBreaker(cfg.CircuitBreakerThreshold, cfg.CircuitBreakerCooldown),
		dlq:     resilience.NewDeadLetterQueue(),
		backpressure: resilience.NewBackpressureController(cfg.Backpressure,
			func() error { return nil },
			func() error { return nil }),
	}

	t.Cleanup(func() { c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := c.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run returned error: %v", err)
	}
}

func TestConsumer_CircuitBreakerOpensAndDegrades(t *testing.T) {
	stubKafka := kafka.NewStubConsumer("test-topic")
	// Seed some messages
	for i := 0; i < 3; i++ {
		stubKafka.MessagesChan() <- &kafka.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    int64(i),
			Key:       "key",
			Value:     "value",
		}
	}
	close(stubKafka.MessagesChan())

	handler := &stubHandler{err: errors.New("downstream failed")}

	c := &Consumer{
		kafka:   stubKafka,
		handler: handler,
		breaker: resilience.NewCircuitBreaker(2, 10*time.Millisecond),
		dlq:     resilience.NewDeadLetterQueue(),
		backpressure: resilience.NewBackpressureController(resilience.DefaultBackpressureConfig(),
			func() error { return nil },
			func() error { return nil }),
	}

	t.Cleanup(func() { c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := c.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run returned error: %v", err)
	}

	// Check that messages went to DLQ
	if c.dlq.Count() != 3 {
		t.Fatalf("expected 3 messages in DLQ, got %d", c.dlq.Count())
	}

	// Check circuit breaker state
	if c.breaker.State() != resilience.StateOpen {
		t.Fatalf("circuit breaker should be open, got %s", c.breaker.State())
	}
}

func TestConsumer_DeadLetterQueueOnError(t *testing.T) {
	stubKafka := kafka.NewStubConsumer("test-topic")
	// Seed one message
	stubKafka.MessagesChan() <- &kafka.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
		Key:       "key",
		Value:     "value",
	}
	close(stubKafka.MessagesChan())

	handler := &stubHandler{err: errors.New("handler error")}

	c := &Consumer{
		kafka:   stubKafka,
		handler: handler,
		breaker: resilience.NewCircuitBreaker(5, 10*time.Millisecond), // high threshold so it doesn't open
		dlq:     resilience.NewDeadLetterQueue(),
		backpressure: resilience.NewBackpressureController(resilience.DefaultBackpressureConfig(),
			func() error { return nil },
			func() error { return nil }),
	}

	t.Cleanup(func() { c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_ = c.Run(ctx)

	// Check that message went to DLQ
	if c.dlq.Count() != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", c.dlq.Count())
	}
	msgs := c.dlq.Messages()
	if msgs[0].Reason != "handler error: handler error" {
		t.Fatalf("unexpected DLQ reason: %s", msgs[0].Reason)
	}
}
