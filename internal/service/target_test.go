package service_test

import (
	"context"
	"testing"
	"time"
	"example.com/backupmesh/internal/service"
)

func TestGcCollectPreservesPinnedSnapshots(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	handle, err := svc.BeginCapture(context.Background(), "snap-p", "node-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 1, []byte("alpha")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CommitCapture(handle, "", true, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	svc.GcPin("snap-p")
	expired := svc.GcCollect(now.Add(time.Hour), 0)
	t.Run("pinned-not-expired", func(t *testing.T) {
		for _, id := range expired {
			if id == "snap-p" {
				t.Fatal("pinned snapshot must not be collected")
			}
		}
	})
}
