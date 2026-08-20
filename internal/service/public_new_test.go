package service_test

import (
	"context"
	"testing"
	"time"

	"example.com/backupmesh/internal/service"
)

func TestWatermarkAdvanceMonotonicPublic(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	if err := svc.AdvanceWatermark("mirror-a", 10, 1); err != nil {
		t.Fatal(err)
	}
	if err := svc.AdvanceWatermark("mirror-a", 20, 1); err != nil {
		t.Fatal(err)
	}
	offset, generation := svc.WatermarkOffset("mirror-a")
	if offset != 20 || generation != 1 {
		t.Fatalf("offset=%d generation=%d", offset, generation)
	}
	if len(svc.Watermarks()) != 1 {
		t.Fatalf("watermarks = %d", len(svc.Watermarks()))
	}
}

func TestMirrorSiteRegistrationPublic(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	if !svc.RegisterMirrorSite("site-east") {
		t.Fatal("first registration should succeed")
	}
	if svc.RegisterMirrorSite("site-east") {
		t.Fatal("duplicate registration should fail")
	}
	if len(svc.MirrorSites()) != 1 || svc.MirrorSites()[0] != "site-east" {
		t.Fatalf("sites = %v", svc.MirrorSites())
	}
}

func TestGcPinAndRefcountPublic(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	svc.GcAddRef("chunk-a", 2)
	svc.GcAddRef("chunk-b", 1)
	svc.GcReleaseRef("chunk-a", 1)
	if svc.GcRefcount("chunk-a") != 1 {
		t.Fatalf("refcount-a = %d", svc.GcRefcount("chunk-a"))
	}
	svc.GcPin("snap-x")
	svc.GcUnpin("snap-x")
}

func TestMigrationVersionPublic(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	if err := svc.MigrateManifest(context.Background(), "snap-m", 0, 3); err != nil {
		t.Fatal(err)
	}
	if svc.MigrationVersion("snap-m") != 3 || !svc.Migrated("snap-m") {
		t.Fatalf("version=%d migrated=%v", svc.MigrationVersion("snap-m"), svc.Migrated("snap-m"))
	}
}

func TestQuarantineAdmitPublic(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	if err := svc.QuarantineChunk("chunk-q", "checksum mismatch"); err != nil {
		t.Fatal(err)
	}
	entry, ok := svc.QuarantineEntry("chunk-q")
	if !ok || entry.Reason != "checksum mismatch" || entry.Verified {
		t.Fatalf("entry = %+v ok=%v", entry, ok)
	}
	if err := svc.ReverifyChunk("chunk-q", true, nil); err != nil {
		t.Fatal(err)
	}
	entry, ok = svc.QuarantineEntry("chunk-q")
	if !ok || !entry.Verified {
		t.Fatalf("entry after reverify = %+v ok=%v", entry, ok)
	}
	if !svc.QuarantineClear("chunk-q") {
		t.Fatal("clear should succeed")
	}
}
