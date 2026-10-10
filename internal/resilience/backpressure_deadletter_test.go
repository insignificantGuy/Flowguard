package resilience

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestBackpressureController_RecordRemove(t *testing.T) {
	cfg := DefaultBackpressureConfig()
	pauseFn := func() error { return nil }
	resumeFn := func() error { return nil }

	bp := NewBackpressureController(cfg, pauseFn, resumeFn)
	defer bp.Stop()

	// Initially depth should be 0 and unpaused
	if depth := bp.Depth(); depth != 0 {
		t.Fatalf("initial depth should be 0, got %d", depth)
	}
	if bp.IsPaused() {
		t.Fatalf("initial state should be unpaused")
	}

	// Record a few messages
	bp.Record()
	bp.Record()
	bp.Record()
	if depth := bp.Depth(); depth != 3 {
		t.Fatalf("after 3 records depth should be 3, got %d", depth)
	}

	// Still not paused (PauseAfterDepth is 800 by default)
	if bp.IsPaused() {
		t.Fatalf("should not be paused after 3 records")
	}
}

func TestBackpressureController_ThrottlesAtPauseDepth(t *testing.T) {
	cfg := BackpressureConfig{
		MaxQueueDepth:      10,
		PauseAfterDepth:    5,
		ResumeAfterDepth:   2,
		CheckInterval:      1 * time.Millisecond,
	}
	pauseCalled := atomic.Bool{}

	bp := NewBackpressureController(cfg, func() error {
		pauseCalled.Store(true)
		return nil
	}, func() error { return nil })
	defer bp.Stop()

	// Records beyond PauseAfterDepth should trigger a pause
	for i := 0; i < 8; i++ {
		bp.Record()
	}

	// Wait for the ticker to fire and call pause
	time.Sleep(3 * cfg.CheckInterval)

	if !pauseCalled.Load() {
		t.Fatal("pause should have been called after depth exceeded PauseAfterDepth")
	}

	// After pause, depth should still be 8
	if depth := bp.Depth(); depth != 8 {
		t.Fatalf("depth after pause should still be 8, got %d", depth)
	}
}

func TestBackpressureController_ResumesAtResumeDepth(t *testing.T) {
	cfg := BackpressureConfig{
		MaxQueueDepth:      10,
		PauseAfterDepth:    5,
		ResumeAfterDepth:   2,
		CheckInterval:      1 * time.Millisecond,
	}

	bp := NewBackpressureController(cfg, func() error { return nil }, func() error { return nil })
	defer bp.Stop()

	// Records beyond PauseAfterDepth
	for i := 0; i < 8; i++ {
		bp.Record()
	}

	// Wait for pause to be called
	time.Sleep(3 * cfg.CheckInterval)

	if !bp.IsPaused() {
		t.Fatal("should be paused after exceeding pause depth")
	}

	// Remove enough messages to go below ResumeAfterDepth
	for i := 0; i < 7; i++ {
		bp.Remove()
	}

	// Give the controller time to detect the depth change
	time.Sleep(3 * cfg.CheckInterval)

	if bp.IsPaused() {
		t.Fatal("should have resumed after depth dropped below ResumeAfterDepth")
	}
}

func TestBackpressureController_IsPausedAndDepth(t *testing.T) {
	cfg := BackpressureConfig{
		MaxQueueDepth:      5,
		PauseAfterDepth:    3,
		ResumeAfterDepth:   1,
		CheckInterval:      1 * time.Millisecond,
	}

	bp := NewBackpressureController(cfg, func() error { return nil }, func() error { return nil })
	defer bp.Stop()

	// Record until paused
	for i := 0; i < 5; i++ {
		bp.Record()
	}

	// Wait for the pause
	time.Sleep(3 * cfg.CheckInterval)

	// With 5 records and PauseAfterDepth=3, it should be paused
	if !bp.IsPaused() {
		t.Fatal("should be paused after recording enough messages")
	}

	// Remove messages until below ResumeAfterDepth
	for i := 0; i < 5; i++ {
		bp.Remove()
		time.Sleep(cfg.CheckInterval)
		if !bp.IsPaused() {
			break
		}
	}

	if bp.IsPaused() {
		t.Fatal("should have resumed after depth dropped below ResumeAfterDepth")
	}
}

func TestDeadLetterQueue_Basic(t *testing.T) {
	q := NewDeadLetterQueue()
	if q.Count() != 0 {
		t.Fatalf("initial count should be 0, got %d", q.Count())
	}

	// Put a message
	err := q.Put("test message", "test reason")
	if err != nil {
		t.Fatalf("Put should not return error: %v", err)
	}
	if q.Count() != 1 {
		t.Fatalf("count should be 1 after Put, got %d", q.Count())
	}

	// Put another message
	err = q.Put("another message", "another reason")
	if err != nil {
		t.Fatalf("second Put should not return error: %v", err)
	}
	if q.Count() != 2 {
		t.Fatalf("count should be 2 after second Put, got %d", q.Count())
	}

	// Get messages
	msgs := q.Messages()
	if len(msgs) != 2 {
		t.Fatalf("should have 2 messages, got %d", len(msgs))
	}

	// Clear
	q.Clear()
	if q.Count() != 0 {
		t.Fatalf("count should be 0 after Clear, got %d", q.Count())
	}
}