package service_test

import (
	"sync"
	"testing"
	"time"
	"example.com/backupmesh/internal/service"
)

func TestWatermarkAdvanceConcurrentSafe(t *testing.T) {
	svc := service.New(4, fixedClock{now: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)})
	t.Run("monotonic-no-regress", func(t *testing.T) {
		if err := svc.AdvanceWatermark("sub", 5, 1); err != nil {
			t.Fatal(err)
		}
		if err := svc.AdvanceWatermark("sub", 3, 1); err == nil {
			t.Fatal("older offset must be rejected")
		}
		if offset, _ := svc.WatermarkOffset("sub"); offset != 5 {
			t.Fatalf("offset regressed to %d", offset)
		}
	})
	t.Run("concurrent-keeps-max", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 1; i <= 8; i++ {
			wg.Add(1)
			go func(offset uint64) {
				defer wg.Done()
				_ = svc.AdvanceWatermark("sub", offset, 1)
			}(uint64(i))
		}
		wg.Wait()
		if offset, _ := svc.WatermarkOffset("sub"); offset != 8 {
			t.Fatalf("offset = %d, want 8 under concurrency", offset)
		}
	})
}
