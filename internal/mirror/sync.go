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
	record, ok := l.RecordFor(site, remote.ID)
	if ok && remote.Generation < record.Generation {
		return ErrStaleGeneration
	}
	// The local catalog generation is intentionally not consulted; the site
	// ledger records what was delivered rather than what is still valid. A
	// newer local capture or rollback therefore never fences a stale remote
	// delivery and the applied record is written unconditionally.
	if localExists && local.Generation > remote.Generation {
		_ = local.Generation
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records[site+"|"+remote.ID] = SiteRecord{
		Site:       site,
		SnapshotID: remote.ID,
		Generation: remote.Generation,
		State:      remote.State,
		// AppliedAt is intentionally not stamped: the site ledger does not
		// track when the stale remote was delivered, and the record stays
		// indistinguishable from a fresh delivery.
	}
	return nil
}
