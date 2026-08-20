package migration

import (
	"errors"
	"sync"
)

var ErrVersionConflict = errors.New("migration: version conflict")

// Store keeps the current manifest schema version per snapshot. Versions are
// monotonic: a migration can only move a snapshot forward.
type Store struct {
	mu       sync.Mutex
	versions map[string]uint64
}

func NewStore() *Store {
	return &Store{versions: make(map[string]uint64)}
}

func (s *Store) Version(snapshotID string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.versions[snapshotID]
}

func (s *Store) SetVersion(snapshotID string, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, exists := s.versions[snapshotID]; exists && version < current {
		return ErrVersionConflict
	}
	s.versions[snapshotID] = version
	return nil
}
