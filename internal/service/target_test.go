package service_test

import (
	"context"
	"testing"
	"time"
	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

func TestMirrorLocalGenerationFencesRemote(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	handle, err := svc.BeginCapture(context.Background(), "snap-r", "node-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 1, []byte("alpha")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := svc.CommitCapture(handle, "", true, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	svc.RegisterMirrorSite("site-west")
	remote := model.Snapshot{ID: snapshot.ID, Generation: snapshot.Generation - 1, State: model.SnapshotPublished}
	t.Run("stale-remote-rejected", func(t *testing.T) {
		if err := svc.ApplyMirrorUpdate("site-west", remote); err == nil {
			t.Fatal("stale remote generation must be rejected")
		}
	})
	t.Run("record-not-written", func(t *testing.T) {
		if len(svc.MirrorRecords()) != 0 {
			t.Fatalf("stale remote recorded: %v", svc.MirrorRecords())
		}
	})
}
