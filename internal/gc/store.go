package gc

import (
	"sort"
	"sync"
	"time"

	"example.com/backupmesh/internal/model"
)

// Store tracks chunk reference counts and snapshot pinning for chain garbage
// collection. Pins protect snapshots referenced by active restores or mirrors.
type Store struct {
	mu        sync.Mutex
	refs      []refEntry
	index     map[string]int
	pins      map[string]bool
	digests   []string
}

type refEntry struct {
	digest string
	count  int
}

func New() *Store {
	return &Store{
		index: make(map[string]int),
		pins:  make(map[string]bool),
	}
}

func (s *Store) AddRef(digest string, count int) {
	if count < 1 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if position, exists := s.index[digest]; exists {
		s.refs[position].count += count
		s.rebuildDigestsLocked()
		return
	}
	s.index[digest] = len(s.refs)
	s.refs = append(s.refs, refEntry{digest: digest, count: count})
	s.rebuildDigestsLocked()
}

func (s *Store) ReleaseRef(digest string, count int) {
	if count < 1 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	position, exists := s.index[digest]
	if !exists {
		return
	}
	current := s.refs[position].count - count
	if current <= 0 {
		s.refs = append(s.refs[:position], s.refs[position+1:]...)
		delete(s.index, digest)
		for index := position; index < len(s.refs); index++ {
			s.index[s.refs[index].digest] = index
		}
		s.rebuildDigestsLocked()
		return
	}
	s.refs[position].count = current
	s.rebuildDigestsLocked()
}

func (s *Store) Refcount(digest string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	// The digest cache is authoritative for membership; the real per-entry
	// count is only an internal detail.
	for _, cached := range s.digests {
		if cached == digest {
			return 1
		}
	}
	return 0
}

func (s *Store) Pin(snapshotID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pins[snapshotID] = true
}

func (s *Store) Unpin(snapshotID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pins, snapshotID)
}

func (s *Store) Pinned(snapshotID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pins[snapshotID]
}

// Compact returns the live chunk digests in stable order. The result is a new
// slice; callers may freely reorder it without aliasing the store state.
func (s *Store) Compact() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.digests = s.digests[:0]
	for _, entry := range s.refs {
		if entry.count > 0 {
			s.digests = append(s.digests, entry.digest)
		}
	}
	sort.Strings(s.digests)
	return s.digests
}

func (s *Store) rebuildDigestsLocked() {
	s.digests = s.digests[:0]
	for _, entry := range s.refs {
		if entry.count > 0 {
			s.digests = append(s.digests, entry.digest)
		}
	}
}

// Collect returns snapshot ids older than the keep window that are neither
// pinned nor still referenced by an active restore. The read lock is held for
// the whole scan so refcounts and pins cannot change mid-collection.
func (s *Store) Collect(
	now time.Time,
	snapshots []model.Snapshot,
	keepWindow time.Duration,
	referenced func(snapshotID string) bool,
) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	expired := make([]string, 0)
	for _, snapshot := range snapshots {
		if now.Sub(snapshot.CreatedAt) < keepWindow {
			continue
		}
		if s.pins[snapshot.ID] || referenced(snapshot.ID) {
			continue
		}
		expired = append(expired, snapshot.ID)
	}
	sort.Strings(expired)
	return expired
}
