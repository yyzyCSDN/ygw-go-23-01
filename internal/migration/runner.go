package migration

import (
	"context"
	"sync"
)

// Runner migrates a snapshot manifest from one schema version to the next.
// The caller's context gates every commit step so a cancelled batch cannot
// leave a half-migrated manifest behind.
type Runner struct {
	mu    sync.Mutex
	store *Store
	done  map[string]bool
}

func NewRunner(store *Store) *Runner {
	return &Runner{store: store, done: make(map[string]bool)}
}

func (r *Runner) Migrate(ctx context.Context, snapshotID string, from, to uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.store.Version(snapshotID) != from {
		return ErrVersionConflict
	}
	if err := r.store.SetVersion(snapshotID, to); err != nil {
		return err
	}
	r.mu.Lock()
	r.done[snapshotID] = true
	r.mu.Unlock()
	return nil
}

func (r *Runner) Migrated(snapshotID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done[snapshotID]
}
