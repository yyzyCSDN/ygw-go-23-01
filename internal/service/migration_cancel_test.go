package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/service"
)

// TestMigrateManifestCancelledContextDoesNotCommit is a regression test for the
// manifest migration cancellation bug. Previously MigrateManifest substituted
// context.Background() for the caller's context, so a cancelled caller still
// had the new schema version committed and received a nil error. The caller's
// context must now reach the migration flow: a cancelled context must not
// commit a new schema version, must not append a journal entry, must not
// record an audit event, and must return the context error.
func TestMigrateManifestCancelledContextDoesNotCommit(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancel()

	err := svc.MigrateManifest(ctx, "snap-cancel", 0, 3)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if version := svc.MigrationVersion("snap-cancel"); version != 0 {
		t.Fatalf("version must remain 0 after a cancelled migration, got %d", version)
	}
	if svc.Migrated("snap-cancel") {
		t.Fatal("snapshot must not be marked migrated after a cancelled migration")
	}
	for _, entry := range svc.JournalEntries() {
		if entry.SnapshotID == "snap-cancel" {
			t.Fatalf("cancelled migration must not append a journal entry: %+v", entry)
		}
	}
	for _, event := range svc.AuditEvents() {
		if event.ObjectID == "snap-cancel" {
			t.Fatalf("cancelled migration must not record an audit event: %+v", event)
		}
	}
}

// TestMigrateManifestLiveContextCommits confirms the gate only blocks
// cancelled contexts: a live context still commits the new schema version,
// records state, and returns no error.
func TestMigrateManifestLiveContextCommits(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})

	if err := svc.MigrateManifest(context.Background(), "snap-live", 0, 3); err != nil {
		t.Fatalf("live context migration: %v", err)
	}
	if version := svc.MigrationVersion("snap-live"); version != 3 {
		t.Fatalf("version = %d, want 3", version)
	}
	if !svc.Migrated("snap-live") {
		t.Fatal("snapshot must be marked migrated on a live context")
	}
}
