package service_test

import (
	"context"
	"testing"
	"time"
	"example.com/backupmesh/internal/service"
)

func TestMigrationCancelledContextNoCommit(t *testing.T) {
	svc := service.New(4, fixedClock{now: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t.Run("cancelled-returns-error", func(t *testing.T) {
		if err := svc.MigrateManifest(ctx, "snap-m", 0, 3); err == nil {
			t.Fatal("cancelled context must fail")
		}
	})
	t.Run("cancelled-does-not-commit-version", func(t *testing.T) {
		if svc.MigrationVersion("snap-m") != 0 {
			t.Fatalf("version advanced to %d", svc.MigrationVersion("snap-m"))
		}
	})
	t.Run("cancelled-no-migrated-marker", func(t *testing.T) {
		if svc.Migrated("snap-m") {
			t.Fatal("migrated marker set despite cancelled context")
		}
	})
}
