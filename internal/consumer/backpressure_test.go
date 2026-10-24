package consumer

import (
	"context"
	"flowguard/internal/kafka"
	"flowguard/internal/observability"
	"flowguard/internal/resilience"
	"sync/atomic"
	"testing"
	"time"
)

type blockingHandler struct{ release chan struct{} }

func (h *blockingHandler) Handle(*kafka.ConsumerMessage) error { <-h.release; return nil }
func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestQueueDepthIncludesWaitingMessagesAndResumes(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		name := "enabled"
		if disabled {
			name = "disabled"
		}
		t.Run(name, func(t *testing.T) {
			source := kafka.NewStubConsumer("work")
			h := &blockingHandler{release: make(chan struct{})}
			var pauses, resumes atomic.Int64
			c := &Consumer{kafka: source, handler: h, breaker: resilience.NewCircuitBreaker(3, time.Second), dlq: resilience.NewDeadLetterQueue(), maxConcurrency: 1, queueCapacity: 8, disableBackpressure: disabled, metrics: observability.NewTestMetrics(), topic: "work"}
			c.backpressure = resilience.NewBackpressureController(resilience.BackpressureConfig{MaxQueueDepth: 8, PauseAfterDepth: 4, ResumeAfterDepth: 1, CheckInterval: time.Millisecond}, func() error {
				if !disabled {
					pauses.Add(1)
				}
				return nil
			}, func() error {
				if !disabled {
					resumes.Add(1)
				}
				return nil
			})
			var ack atomic.Int64
			for i := 0; i < 8; i++ {
				source.MessagesChan() <- &kafka.ConsumerMessage{Topic: "work", Ack: func() { ack.Add(1) }}
			}
			close(source.MessagesChan())
			done := make(chan error, 1)
			go func() { done <- c.Run(context.Background()) }()
			eventually(t, func() bool {
				if disabled {
					return c.BackpressureDepth() == 8
				}
				return pauses.Load() == 1
			})
			if !disabled && c.BackpressureDepth() != 4 {
				t.Fatalf("depth %d, expected queued+executing=4", c.BackpressureDepth())
			}
			close(h.release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("consumer failed to drain")
			}
			if ack.Load() != 8 || c.BackpressureDepth() != 0 {
				t.Fatalf("ack=%d depth=%d", ack.Load(), c.BackpressureDepth())
			}
			if !disabled && resumes.Load() == 0 {
				t.Fatal("did not resume")
			}
			if disabled && pauses.Load() != 0 {
				t.Fatal("disabled controller paused intake")
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
