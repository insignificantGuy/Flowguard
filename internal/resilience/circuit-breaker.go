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
// This file implements the circuit breaker. The other two mechanisms live
// in backpressure.go and dead-letter.go.
package resilience

import (
	"errors"
	"log"
	"sync"
	"time"
)

// CircuitBreakerState is the current state of the circuit breaker.
type CircuitBreakerState int

const (
	// StateClosed means the breaker is closed — calls pass through normally.
	StateClosed CircuitBreakerState = iota
	// StateOpen means the breaker is open — calls are rejected immediately
	// without invoking the downstream dependency.
	StateOpen
	// StateHalfOpen means the breaker is testing whether the downstream
	// has recovered by allowing a single trial call through.
	StateHalfOpen
)

func (s CircuitBreakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	}
	return "unknown"
}

// ErrCircuitBreakerOpen is returned when a call is rejected because the
// circuit breaker is open.
var ErrCircuitBreakerOpen = errors.New("circuit breaker is open")

// CircuitBreaker protects a downstream dependency from repeated failures.
//
// When the number of consecutive failures reaches the configured threshold,
// the breaker opens and rejects further calls for a cooldown period. After
// the cooldown, it transitions to half-open and allows a single trial call;
// if that call succeeds the breaker closes, if it fails the breaker opens
// again.
//
// The breaker is safe for concurrent use.
type CircuitBreaker struct {
	mu                  sync.Mutex
	state               CircuitBreakerState
	consecutiveFailures int
	lastFailureTime     time.Time
	opensAt             time.Time
	threshold           int
	cooldown            time.Duration
	generation          uint64
}

// NewCircuitBreaker creates a circuit breaker with the given failure threshold
// and cooldown period.
func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	if threshold < 1 {
		threshold = 1
	}
	if cooldown < 0 {
		cooldown = 0
	}
	return &CircuitBreaker{
		state:     StateClosed,
		threshold: threshold,
		cooldown:  cooldown,
	}
}

// Execute runs fn through the circuit breaker.
//
// If the breaker is open and the cooldown has not elapsed, the call is
// rejected immediately with ErrCircuitBreakerOpen. If the breaker is open
// and the cooldown has elapsed, the breaker transitions to half-open and
// allows the call through as a trial.
//
// If fn returns an error, the breaker records a failure. If fn succeeds,
// the breaker records a success and closes if it was half-open.
// transition runs under mu, so logs include even brief half-open transitions.
func (cb *CircuitBreaker) transition(state CircuitBreakerState) {
	if cb.state == state {
		return
	}
	log.Printf("circuit_breaker from=%s to=%s", cb.state, state)
	cb.state = state
	cb.generation++
}
func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	if cb.state == StateOpen {
		if time.Since(cb.opensAt) < cb.cooldown {
			cb.mu.Unlock()
			return ErrCircuitBreakerOpen
		}
		cb.transition(StateHalfOpen)
	} else if cb.state == StateHalfOpen {
		cb.mu.Unlock()
		return ErrCircuitBreakerOpen
	}
	generation := cb.generation
	trial := cb.state == StateHalfOpen
	cb.mu.Unlock()
	err := fn()
	cb.mu.Lock()
	defer cb.mu.Unlock()
	// Ignore results from calls admitted before a state change.
	if generation != cb.generation {
		return err
	}
	if err != nil {
		cb.consecutiveFailures++
		if trial || cb.consecutiveFailures >= cb.threshold {
			cb.transition(StateOpen)
			cb.opensAt = time.Now()
		}
	} else {
		cb.consecutiveFailures = 0
		if trial {
			cb.transition(StateClosed)
		}
	}
	return err
}

// State returns the current state of the circuit breaker.
func (cb *CircuitBreaker) State() CircuitBreakerState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// ConsecutiveFailures returns the current count of consecutive failures.
func (cb *CircuitBreaker) ConsecutiveFailures() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.consecutiveFailures
}

// Reset manually resets the breaker to the closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transition(StateClosed)
	cb.consecutiveFailures = 0
}

// ForceOpen manually opens the breaker (useful for testing).
func (cb *CircuitBreaker) ForceOpen() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transition(StateOpen)
	cb.opensAt = time.Now()
}

// ForceHalfOpen manually transitions the breaker to half-open (useful for testing).
func (cb *CircuitBreaker) ForceHalfOpen() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transition(StateHalfOpen)
}
