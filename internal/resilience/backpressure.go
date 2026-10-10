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
// This file implements backpressure. The other two mechanisms live in
// circuit-breaker.go and dead-letter.go.
package resilience

import (
	"sync"
	"sync/atomic"
	"time"
)

// BackpressureConfig controls the backpressure behavior.
type BackpressureConfig struct {
	// MaxQueueDepth is the maximum number of messages allowed in the
	// consumer's queue before backpressure kicks in.
	MaxQueueDepth int
	// PauseAfterDepth triggers a pause of intake when the queue depth
	// reaches this threshold.
	PauseAfterDepth int
	// ResumeAfterDepth triggers a resume of intake when the queue depth
	// drops below this threshold.
	ResumeAfterDepth int
	// CheckInterval is how often the backpressure controller checks the
	// queue depth.
	CheckInterval time.Duration
}

// DefaultBackpressureConfig returns a sensible default configuration.
func DefaultBackpressureConfig() BackpressureConfig {
	return BackpressureConfig{
		MaxQueueDepth:    1000,
		PauseAfterDepth:  800,
		ResumeAfterDepth: 200,
		CheckInterval:    100 * time.Millisecond,
	}
}

// BackpressureController monitors a queue depth and pauses/resumes intake
// based on the configured thresholds.
//
// When the queue depth exceeds PauseAfterDepth, the controller calls Pause()
// on the underlying consumer to stop fetching new messages. When the depth
// drops below ResumeAfterDepth, the controller calls Resume() to allow
// fetching to continue.
//
// The controller is safe for concurrent use.
type BackpressureController struct {
	cfg        BackpressureConfig
	pauseFn    func() error
	resumeFn   func() error
	depth      atomic.Int64
	paused     atomic.Bool
	stopCh     chan struct{}
	mu         sync.Mutex
}

// NewBackpressureController creates a backpressure controller.
//
// pauseFn and resumeFn are called when the controller decides to pause or
// resume intake. They typically delegate to the underlying consumer's
// Pause()/Resume() methods.
func NewBackpressureController(cfg BackpressureConfig, pauseFn, resumeFn func() error) *BackpressureController {
	if cfg.MaxQueueDepth <= 0 {
		cfg.MaxQueueDepth = 1000
	}
	if cfg.PauseAfterDepth <= 0 {
		cfg.PauseAfterDepth = cfg.MaxQueueDepth
	}
	if cfg.ResumeAfterDepth <= 0 {
		cfg.ResumeAfterDepth = cfg.MaxQueueDepth / 4
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 100 * time.Millisecond
	}

	bp := &BackpressureController{
		cfg:      cfg,
		pauseFn:  pauseFn,
		resumeFn: resumeFn,
		stopCh:   make(chan struct{}),
	}
	bp.paused.Store(false)

	go bp.run()

	return bp
}

// run is the background goroutine that periodically checks the queue depth
// and pauses/resumes intake accordingly.
func (bp *BackpressureController) run() {
	ticker := time.NewTicker(bp.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bp.stopCh:
			return
		case <-ticker.C:
			bp.check()
		}
	}
}

// check evaluates the current queue depth and decides whether to pause or
// resume intake.
func (bp *BackpressureController) check() {
	depth := bp.depth.Load()
	paused := bp.paused.Load()

	if depth >= int64(bp.cfg.PauseAfterDepth) && !paused {
		bp.mu.Lock()
		if err := bp.pauseFn(); err != nil {
			// Log the error but don't crash — backpressure is best-effort
		}
		bp.mu.Unlock()
		bp.paused.Store(true)
	} else if depth <= int64(bp.cfg.ResumeAfterDepth) && paused {
		bp.mu.Lock()
		if err := bp.resumeFn(); err != nil {
			// Log the error but don't crash
		}
		bp.mu.Unlock()
		bp.paused.Store(false)
	}
}

// Record records a new message in the queue depth counter.
func (bp *BackpressureController) Record() {
	bp.depth.Add(1)
}

// Remove removes a message from the queue depth counter.
func (bp *BackpressureController) Remove() {
	bp.depth.Add(-1)
}

// Depth returns the current queue depth.
func (bp *BackpressureController) Depth() int64 {
	return bp.depth.Load()
}

// IsPaused returns whether intake is currently paused.
func (bp *BackpressureController) IsPaused() bool {
	return bp.paused.Load()
}

// Stop stops the backpressure controller.
func (bp *BackpressureController) Stop() {
	close(bp.stopCh)
}