package service_test

import (
	"testing"
	"time"
	"example.com/backupmesh/internal/service"
)

func TestGcCompactDoesNotAliasBacking(t *testing.T) {
	svc := service.New(4, fixedClock{now: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)})
	svc.GcAddRef("chunk-a", 1)
	svc.GcAddRef("chunk-b", 1)
	t.Run("compact-returns-complete-list", func(t *testing.T) {
		got := svc.GcCompact()
		if len(got) != 2 || got[0] != "chunk-a" || got[1] != "chunk-b" {
			t.Fatalf("compact list = %v", got)
		}
	})
	t.Run("release-does-not-mutate-retained", func(t *testing.T) {
		before := svc.GcCompact()
		svc.GcReleaseRef("chunk-a", 1)
		if len(before) != 2 || before[0] != "chunk-a" || before[1] != "chunk-b" {
			t.Fatalf("retained list mutated by later release: %v", before)
		}
	})
}
