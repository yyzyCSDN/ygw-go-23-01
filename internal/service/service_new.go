package service

import (
	"context"
	"time"

	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/migration"
	"example.com/backupmesh/internal/mirror"
	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/quarantine"
	"example.com/backupmesh/internal/watermark"
)

// Watermark progress ---------------------------------------------------------

func (s *Service) AdvanceWatermark(subscriber string, offset, generation uint64) error {
	return s.watermark.Advance(subscriber, offset, generation)
}

func (s *Service) WatermarkOffset(subscriber string) (uint64, uint64) {
	return s.watermark.Offset(subscriber)
}

func (s *Service) Watermarks() []watermark.Entry {
	return s.watermark.Snapshot()
}

// Mirror synchronization -----------------------------------------------------

func (s *Service) RegisterMirrorSite(site string) bool {
	return s.mirror.Register(site)
}

func (s *Service) MirrorSites() []string {
	return s.mirror.Sites()
}

func (s *Service) MirrorRecords() []mirror.SiteRecord {
	return s.mirror.Snapshot()
}

func (s *Service) ApplyMirrorUpdate(site string, snapshot model.Snapshot) error {
	local, localExists := s.catalog.Snapshot(snapshot.ID)
	if err := s.mirror.Apply(site, snapshot, local, localExists, s.clock.Now()); err != nil {
		return err
	}
	s.audit.Record("mirror-applied", snapshot.ID, site, s.clock.Now())
	return nil
}

// Chain garbage collection ---------------------------------------------------

func (s *Service) GcPin(snapshotID string)   { s.gcStore.Pin(snapshotID) }
func (s *Service) GcUnpin(snapshotID string) { s.gcStore.Unpin(snapshotID) }
func (s *Service) GcPinned(snapshotID string) bool {
	return s.gcStore.Pinned(snapshotID)
}
func (s *Service) GcAddRef(digest string, count int)     { s.gcStore.AddRef(digest, count) }
func (s *Service) GcReleaseRef(digest string, count int) { s.gcStore.ReleaseRef(digest, count) }
func (s *Service) GcRefcount(digest string) int          { return s.gcStore.Refcount(digest) }
func (s *Service) GcCompact() []string                   { return s.gcStore.Compact() }

func (s *Service) GcCollect(now time.Time, keepWindow time.Duration) []string {
	s.gcMu.Lock()
	defer s.gcMu.Unlock()
	for _, record := range s.mirror.Snapshot() {
		s.gcStore.Pin(record.SnapshotID)
	}
	snapshots := []model.Snapshot{}
	for _, id := range s.gcSnapshotIDs() {
		if snapshot, ok := s.catalog.Snapshot(id); ok {
			snapshots = append(snapshots, snapshot)
		}
	}
	return s.gcStore.Collect(now, snapshots, keepWindow, func(id string) bool {
		return s.catalog.HasRestoreRef(id)
	})
}

func (s *Service) gcSnapshotIDs() []string {
	ids := make(map[string]struct{})
	for _, record := range s.mirror.Snapshot() {
		ids[record.SnapshotID] = struct{}{}
	}
	for _, event := range s.journal.Entries() {
		if event.SnapshotID != "" {
			ids[event.SnapshotID] = struct{}{}
		}
	}
	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	return result
}

// Manifest schema migration --------------------------------------------------

func (s *Service) MigrateManifest(ctx context.Context, snapshotID string, from, to uint64) error {
	if err := s.migration.Migrate(ctx, snapshotID, from, to); err != nil {
		return err
	}
	if _, err := s.journal.Append(journal.Entry{
		Generation: to,
		Kind:       "migration",
		SnapshotID: snapshotID,
		Operation:  "migrate-manifest",
		Detail:     "schema migrated",
	}); err != nil {
		return err
	}
	s.audit.Record("manifest-migrated", snapshotID, "schema", s.clock.Now())
	return nil
}

func (s *Service) MigrationVersion(snapshotID string) uint64 {
	return s.migrationVersionStore().Version(snapshotID)
}

func (s *Service) Migrated(snapshotID string) bool {
	return s.migration.Migrated(snapshotID)
}

func (s *Service) migrationVersionStore() *migration.Store {
	// The runner owns the store; expose reads through a small accessor.
	return s.migrationStore
}

// Quarantine -----------------------------------------------------------------

func (s *Service) QuarantineChunk(digest, reason string) error {
	// Durability first: the journal append must succeed before the in-memory
	// entry is allowed to exist. A closed (or otherwise failing) journal
	// therefore rejects the admission outright and leaves nothing behind, so
	// callers can never observe a quarantine that was not first made durable.
	if _, err := s.journal.Append(journal.Entry{
		Kind:       "quarantine-admit",
		SnapshotID: digest,
		Operation:  "quarantine",
		Detail:     reason,
	}); err != nil {
		return err
	}
	if err := s.quarantine.Admit(digest, reason, s.clock.Now()); err != nil {
		return err
	}
	s.audit.Record("chunk-quarantined", digest, reason, s.clock.Now())
	return nil
}

func (s *Service) ReverifyChunk(digest string, matches bool, verifyErr error) error {
	return s.quarantine.Reverify(digest, matches, verifyErr, s.clock.Now())
}

func (s *Service) QuarantineEntry(digest string) (quarantine.Entry, bool) {
	return s.quarantine.Entry(digest)
}

func (s *Service) QuarantineClear(digest string) bool {
	return s.quarantine.Clear(digest)
}
