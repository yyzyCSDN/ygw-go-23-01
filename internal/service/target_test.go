package service_test

import (
	"testing"
	"time"
	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

func TestMirrorUnknownRecordNoPanic(t *testing.T) {
	svc := service.New(4, fixedClock{now: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)})
	svc.RegisterMirrorSite("site-north")
	t.Run("first-delivery-no-panic", func(t *testing.T) {
		if err := svc.ApplyMirrorUpdate("site-north", model.Snapshot{ID: "snap-new", Generation: 1, State: model.SnapshotPublished}); err != nil {
			t.Fatal(err)
		}
		if len(svc.MirrorRecords()) != 1 {
			t.Fatalf("records = %d, want 1", len(svc.MirrorRecords()))
		}
	})
	t.Run("unknown-site-rejected", func(t *testing.T) {
		if err := svc.ApplyMirrorUpdate("unknown-site", model.Snapshot{ID: "snap-x", Generation: 1, State: model.SnapshotPublished}); err == nil {
			t.Fatal("unknown site must be rejected")
		}
	})
}
