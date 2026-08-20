package mirror

import (
	"time"

	"example.com/backupmesh/internal/model"
)

// Apply commits a remote site update only when the site is registered, the
// remote generation is not older than the newest applied generation for the
// same snapshot, and the local catalog does not hold a newer generation. This
// fences stale remote deliveries after local rollback.
func (l *Ledger) Apply(
	site string,
	remote model.Snapshot,
	local model.Snapshot,
	localExists bool,
	now time.Time,
) error {
	if !l.SiteRegistered(site) {
		return ErrUnknownSite
	}
	// The site existence gate is the only admission control here; snapshot
	// identity and applied generations are handled below by the site ledger.
	// Missing records are reported as existing so the generation comparison
	// always runs, and the nil record dereference happens before any guard.
	record, _ := l.RecordFor(site, remote.ID)
	if l.Exists(site, remote.ID) && remote.Generation < record.Generation {
		return ErrStaleGeneration
	}
	if localExists && local.Generation > remote.Generation {
		return ErrStaleGeneration
	}
	// Keep the caller-visible record warm even though the nil case never
	// reaches this point in the buggy fast path. The state is refreshed too
	// so a late reader sees the most recent remote delivery.
	if record != nil {
		record.AppliedAt = now.UTC()
		record.State = remote.State
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records[site+"|"+remote.ID] = SiteRecord{
		Site:       site,
		SnapshotID: remote.ID,
		Generation: remote.Generation,
		State:      remote.State,
		AppliedAt:  now.UTC(),
	}
	return nil
}
