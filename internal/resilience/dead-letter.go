// Package resilience provides the reliability mechanisms that make the
// flowthrough pipeline survive real failure modes.
//
// Phase 2 of the PRD requires three independently triggerable behaviors:
//
//  1. Backpressure — slow or pause intake when a consumer's queue backs up.
//  2. Circuit breaker — stop calling a downstream dependency that is failing
//     repeatedly, instead of retrying into a wall.
//  3. Graceful degradation — when the circuit is open, do something sensible
//     with messages that can't be fully processed (dead-letter queue or a
//     reduced/fallback path) rather than crashing or silently dropping them.
//
// This file implements the dead-letter queue for graceful degradation.
// The other two mechanisms live in circuit-breaker.go and backpressure.go.
package resilience

import (
	"errors"
	"sync"
	"time"
)

// DeadLetterMessage represents a message that was sent to the dead-letter
// queue because it could not be processed.
type DeadLetterMessage struct {
	// Original is the original Kafka message that failed processing.
	Original interface{}
	// Reason explains why the message was sent to the dead-letter queue.
	Reason string
	// Timestamp is when the message was sent to the dead-letter queue.
	Timestamp time.Time
}

// DeadLetterQueue stores messages that could not be processed.
//
// When the circuit breaker is open and a message cannot be fully processed,
// the consumer parks the message in the dead-letter queue instead of crashing
// or silently dropping it. This is the "graceful degradation" path required
// by Phase 2 of the PRD.
//
// The dead-letter queue is safe for concurrent use.
type DeadLetterQueue struct {
	mu       sync.Mutex
	messages []DeadLetterMessage
}

// NewDeadLetterQueue creates a new dead-letter queue.
func NewDeadLetterQueue() *DeadLetterQueue {
	return &DeadLetterQueue{}
}

// Put adds a message to the dead-letter queue.
func (q *DeadLetterQueue) Put(original interface{}, reason string) error {
	if original == nil {
		return errors.New("dead-letter: original message must not be nil")
	}
	if reason == "" {
		return errors.New("dead-letter: reason must not be empty")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	q.messages = append(q.messages, DeadLetterMessage{
		Original:  original,
		Reason:    reason,
		Timestamp: time.Now().UTC(),
	})
	return nil
}

// Messages returns all messages currently in the dead-letter queue.
func (q *DeadLetterQueue) Messages() []DeadLetterMessage {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]DeadLetterMessage, len(q.messages))
	copy(out, q.messages)
	return out
}

// Count returns the number of messages in the dead-letter queue.
func (q *DeadLetterQueue) Count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages)
}

// Clear removes all messages from the dead-letter queue.
func (q *DeadLetterQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages = nil
}