package migration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/migration"
)

// TestMigrateCancelledContextDoesNotCommit is a regression test for the manifest
// migration cancellation bug. The caller's context must gate the schema
// version commit: when the context is already cancelled, Migrate must not
// advance the manifest version and must surface the context error instead.
func TestMigrateCancelledContextDoesNotCommit(t *testing.T) {
	store := migration.NewStore()
	runner := migration.NewRunner(store)
	// Seed the snapshot at version 1 so the from-check would pass; the only
	// thing then standing between the caller and a new version is the context.
	if err := store.SetVersion("snap-a", 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancel()

	err := runner.Migrate(ctx, "snap-a", 1, 2)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if version := store.Version("snap-a"); version != 1 {
		t.Fatalf("version must remain 1 after a cancelled migration, got %d", version)
	}
	if runner.Migrated("snap-a") {
		t.Fatal("snapshot must not be marked migrated after a cancelled migration")
	}
	if runner.Pending("snap-a") {
		t.Fatal("snapshot must not be marked pending after a cancelled migration")
	}
}

// TestMigrateExpiredDeadlineDoesNotCommit confirms any context error — not just
// cancellation — gates the commit. An already-expired deadline must skip the
// SetVersion write and return context.DeadlineExceeded.
func TestMigrateExpiredDeadlineDoesNotCommit(t *testing.T) {
	store := migration.NewStore()
	runner := migration.NewRunner(store)
	if err := store.SetVersion("snap-d", 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()

	err := runner.Migrate(ctx, "snap-d", 1, 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if version := store.Version("snap-d"); version != 1 {
		t.Fatalf("version must remain 1 after an expired deadline, got %d", version)
	}
	if runner.Migrated("snap-d") {
		t.Fatal("snapshot must not be marked migrated after an expired deadline")
	}
}

// TestMigrateLiveContextCommits confirms the gate only blocks cancelled
// contexts: a live context still commits the new version and records state.
func TestMigrateLiveContextCommits(t *testing.T) {
	store := migration.NewStore()
	runner := migration.NewRunner(store)
	if err := store.SetVersion("snap-b", 1); err != nil {
		t.Fatal(err)
	}
	if err := runner.Migrate(context.Background(), "snap-b", 1, 2); err != nil {
		t.Fatalf("live context migration: %v", err)
	}
	if version := store.Version("snap-b"); version != 2 {
		t.Fatalf("version = %d, want 2", version)
	}
	if !runner.Migrated("snap-b") {
		t.Fatal("snapshot must be marked migrated on a live context")
	}
	if !runner.Pending("snap-b") {
		t.Fatal("snapshot must be marked pending on a live context")
	}
}
