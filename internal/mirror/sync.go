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
	if localExists && local.Generation > remote.Generation {
		return ErrStaleGeneration
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
