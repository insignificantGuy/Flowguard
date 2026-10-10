package resilience

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreaker_ExecuteSuccess(t *testing.T) {
	cb := NewCircuitBreaker(3, 100*time.Millisecond)

	var count int
	fn := func() error {
		count++
		return nil
	}

	// Three successful calls should keep the breaker closed
	for i := 0; i < 3; i++ {
		if err := cb.Execute(fn); err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
		if cb.State() != StateClosed {
			t.Fatalf("call %d: breaker state should be closed, got %s", i, cb.State())
		}
	}
	if count != 3 {
		t.Fatalf("expected 3 calls, got %d", count)
	}
}

func TestCircuitBreaker_ExecuteFailureOpens(t *testing.T) {
	cb := NewCircuitBreaker(3, 100*time.Millisecond)

	fn := func() error { return errors.New("boom") }

	// First two failures should keep the breaker closed
	for i := 0; i < 2; i++ {
		if err := cb.Execute(fn); err == nil {
			t.Fatalf("call %d: expected error, got nil", i)
		}
		if cb.State() != StateClosed {
			t.Fatalf("call %d: breaker should remain closed after %d failures, got %s", i, i+1, cb.State())
		}
	}

	// Third failure should open the breaker
	if err := cb.Execute(fn); err == nil {
		t.Fatalf("call 2: expected error, got nil")
	}
	if cb.State() != StateOpen {
		t.Fatalf("after 3 failures breaker should be open, got %s", cb.State())
	}
}

func TestCircuitBreaker_OpenRejects(t *testing.T) {
	cb := NewCircuitBreaker(2, 10*time.Millisecond)

	fn := func() error { return errors.New("boom") }

	// Two failures open the breaker
	for i := 0; i < 2; i++ {
		if err := cb.Execute(fn); err == nil {
			t.Fatalf("call %d: expected error, got nil", i)
		}
	}

	// Now the breaker should reject calls
	for i := 0; i < 5; i++ {
		if err := cb.Execute(fn); err != ErrCircuitBreakerOpen {
			t.Fatalf("call %d: expected ErrCircuitBreakerOpen, got %v", i, err)
		}
	}
}

func TestCircuitBreaker_HalfOpenAllowsTrial(t *testing.T) {
	cb := NewCircuitBreaker(2, 10*time.Millisecond)

	failFn := func() error { return errors.New("boom") }
	passFn := func() error { return nil }

	// Two failures open the breaker
	for i := 0; i < 2; i++ {
		if err := cb.Execute(failFn); err == nil {
			t.Fatalf("fail call %d: expected error, got nil", i)
		}
	}

	// Wait for cooldown to allow half-open trial
	time.Sleep(15 * time.Millisecond)

	// One successful call should close the breaker
	if err := cb.Execute(passFn); err != nil {
		t.Fatalf("half-open call: unexpected error: %v", err)
	}
	if cb.State() != StateClosed {
		t.Fatalf("after half-open success breaker should be closed, got %s", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(2, 10*time.Millisecond)

	failFn := func() error { return errors.New("boom") }

	// Two failures open the breaker
	for i := 0; i < 2; i++ {
		if err := cb.Execute(failFn); err == nil {
			t.Fatalf("fail call %d: expected error, got nil", i)
		}
	}

	// Wait for cooldown to allow half-open trial
	time.Sleep(15 * time.Millisecond)

	// A failure in half-open should reopen the breaker
	if err := cb.Execute(failFn); err == nil {
		t.Fatalf("half-open call: expected error, got nil")
	}
	if cb.State() != StateOpen {
		t.Fatalf("after half-open failure breaker should be open, got %s", cb.State())
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cb := NewCircuitBreaker(3, 10*time.Millisecond)

	failFn := func() error { return errors.New("boom") }

	// Open the breaker
	for i := 0; i < 3; i++ {
		if err := cb.Execute(failFn); err == nil {
			t.Fatalf("call %d: expected error, got nil", i)
		}
	}
	if cb.State() != StateOpen {
		t.Fatalf("after 3 failures breaker should be open, got %s", cb.State())
	}

	// Reset should close it
	cb.Reset()
	if cb.State() != StateClosed {
		t.Fatalf("after Reset breaker should be closed, got %s", cb.State())
	}
	if cb.ConsecutiveFailures() != 0 {
		t.Fatalf("after Reset consecutive failures should be 0, got %d", cb.ConsecutiveFailures())
	}
}

func TestCircuitBreaker_ForceOpenAndHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(3, 10*time.Millisecond)

	// Start closed
	if cb.State() != StateClosed {
		t.Fatalf("initial state should be closed, got %s", cb.State())
	}

	// Force open
	cb.ForceOpen()
	if cb.State() != StateOpen {
		t.Fatalf("after ForceOpen state should be open, got %s", cb.State())
	}

	// Force half-open
	cb.ForceHalfOpen()
	if cb.State() != StateHalfOpen {
		t.Fatalf("after ForceHalfOpen state should be half-open, got %s", cb.State())
	}
}