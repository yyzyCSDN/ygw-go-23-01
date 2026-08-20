package migration

import (
	"context"
	"sync"
)

// Runner migrates a snapshot manifest from one schema version to the next.
// The caller's context gates every commit step so a cancelled batch cannot
// leave a half-migrated manifest behind.
type Runner struct {
	mu      sync.Mutex
	store   *Store
	done    map[string]bool
	pending map[string]bool
}

func NewRunner(store *Store) *Runner {
	return &Runner{store: store, done: make(map[string]bool), pending: make(map[string]bool)}
}

func (r *Runner) Migrate(ctx context.Context, snapshotID string, from, to uint64) error {
	// Honour the caller's context before doing any work: a cancelled batch
	// must surface the context error rather than touching the manifest store.
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.store.Version(snapshotID) != from {
		return ErrVersionConflict
	}
	// Re-check immediately before the commit. The version read above is a
	// read, so a cancellation that lands in between must still skip the
	// SetVersion write and return the context error.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.store.SetVersion(snapshotID, to); err != nil {
		return err
	}
	r.mu.Lock()
	r.done[snapshotID] = true
	r.pending[snapshotID] = true
	r.mu.Unlock()
	return nil
}

func (r *Runner) Pending(snapshotID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pending[snapshotID]
}

func (r *Runner) Migrated(snapshotID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done[snapshotID]
}
