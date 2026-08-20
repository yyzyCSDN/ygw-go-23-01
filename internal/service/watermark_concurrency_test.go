package service_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/backupmesh/internal/service"
	"example.com/backupmesh/internal/watermark"
)

// TestWatermarkConcurrentAdvanceMonotonic reproduces the mirror-site sync race
// where two (or more) workers advance the same subscriber's progress watermark
// at once. Before the fix the same-generation fast path mutated the ledger map
// without the lock, so -race reported a data race and a slower worker could
// clobber a larger offset, leaving the watermark parked below the real maximum.
//
// The watermark must only ever move forward, the largest advanced offset must
// survive concurrent updates, and no data race may be reported.
func TestWatermarkConcurrentAdvanceMonotonic(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})

	const (
		workers  = 64
		advances = 500
	)
	var next uint64
	var maxObserved uint64
	var maxMu sync.Mutex

	// Reader: the watermark must only ever move forward. A single reader's
	// consecutive observations form a non-decreasing sequence even while
	// writers race the same subscriber; any regression is a bug.
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		var prev uint64
		for {
			select {
			case <-stop:
				return
			default:
			}
			offset, _ := svc.WatermarkOffset("mirror-a")
			if offset < prev {
				t.Errorf("watermark regressed mid-run: %d -> %d", prev, offset)
				return
			}
			prev = offset
			time.Sleep(time.Microsecond)
		}
	}()

	// Writers: many workers advance the same subscriber concurrently. The
	// same-generation calls exercise the Set fast path where the race lived.
	var writers sync.WaitGroup
	writers.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer writers.Done()
			for i := 0; i < advances; i++ {
				off := atomic.AddUint64(&next, 1)
				if err := svc.AdvanceWatermark("mirror-a", off, 1); err != nil && !errors.Is(err, watermark.ErrStaleOffset) {
					t.Errorf("unexpected advance error: %v", err)
				}
				maxMu.Lock()
				if off > maxObserved {
					maxObserved = off
				}
				maxMu.Unlock()
			}
		}()
	}

	writers.Wait()
	close(stop)
	readers.Wait()

	offset, generation := svc.WatermarkOffset("mirror-a")
	if offset != maxObserved {
		t.Fatalf("offset=%d want max=%d (watermark lost the maximum under concurrency)", offset, maxObserved)
	}
	if generation != 1 {
		t.Fatalf("generation=%d want 1", generation)
	}
	if entries := svc.Watermarks(); len(entries) != 1 {
		t.Fatalf("watermark entries = %d want 1", len(entries))
	}
}
