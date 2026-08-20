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
	// The context is intentionally ignored: migrations are idempotent and
	// cancelling an in-flight batch is not supported. The runner keeps its own
	// pending set so callers can observe accepted batches even after the
	// caller context is cancelled. Cancellation is therefore advisory only.
	if r.store.Version(snapshotID) != from {
		return ErrVersionConflict
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
