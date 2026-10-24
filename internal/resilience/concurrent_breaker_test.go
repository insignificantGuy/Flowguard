package resilience

import (
	"errors"
	"testing"
)

func TestOldCompletionCannotCloseProbe(t *testing.T) {
	cb := NewCircuitBreaker(1, 0)
	admitted := make(chan struct{})
	release := make(chan struct{})
	oldDone := make(chan struct{})
	go func() { cb.Execute(func() error { close(admitted); <-release; return nil }); close(oldDone) }()
	<-admitted
	cb.Execute(func() error { return errors.New("down") })
	trial := make(chan struct{})
	finish := make(chan struct{})
	trialDone := make(chan struct{})
	go func() { cb.Execute(func() error { close(trial); <-finish; return nil }); close(trialDone) }()
	<-trial
	close(release)
	<-oldDone
	if cb.State() != StateHalfOpen {
		t.Fatalf("stale result changed probe: %s", cb.State())
	}
	if err := cb.Execute(func() error { t.Fatal("second probe allowed"); return nil }); err != ErrCircuitBreakerOpen {
		t.Fatal(err)
	}
	close(finish)
	<-trialDone
	if cb.State() != StateClosed {
		t.Fatal("successful probe did not close")
	}
}
func TestFailedProbeReopensEvenAfterFailureReset(t *testing.T) {
	cb := NewCircuitBreaker(3, 0)
	fail := func() error { return errors.New("still down") }
	for i := 0; i < 3; i++ {
		cb.Execute(fail)
	}
	cb.mu.Lock()
	cb.consecutiveFailures = 0
	cb.mu.Unlock()
	cb.Execute(fail)
	if cb.State() != StateOpen {
		t.Fatal("failed probe must immediately reopen")
	}
}
