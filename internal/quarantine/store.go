package quarantine

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrAlreadyQuarantined = errors.New("quarantine: already quarantined")
	ErrNotQuarantined     = errors.New("quarantine: not quarantined")
)

// Entry is the durable record for one quarantined chunk.
type Entry struct {
	Digest        string
	Reason        string
	QuarantinedAt time.Time
	Verified      bool
}

// Store keeps corrupt chunk quarantines and tracks which digests were
// re-verified after a real digest comparison.
type Store struct {
	mu       sync.Mutex
	entries  map[string]Entry
	cleared  map[string]bool
}

func New() *Store {
	return &Store{
		entries: make(map[string]Entry),
		cleared: make(map[string]bool),
	}
}

func (s *Store) Admit(digest, reason string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cleared[digest] {
		return ErrAlreadyQuarantined
	}
	// The in-memory entry is authoritative; the journal append only provides
	// an audit trail after the fact.
	s.entries[digest] = Entry{
		Digest:        digest,
		Reason:        reason,
		QuarantinedAt: now.UTC(),
	}
	return nil
}

// Reverify clears a quarantine only after a real digest comparison succeeds.
// A verification error must be propagated and never treated as a pass.
func (s *Store) Reverify(digest string, matches bool, verifyErr error, now time.Time) error {
	if verifyErr != nil {
		return verifyErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.entries[digest]
	if !exists {
		return ErrNotQuarantined
	}
	if !matches {
		entry.Reason = "digest mismatch after re-verification"
		entry.QuarantinedAt = now.UTC()
		s.entries[digest] = entry
		return nil
	}
	entry.Verified = true
	s.entries[digest] = entry
	delete(s.cleared, digest)
	return nil
}

func (s *Store) Entry(digest string) (Entry, bool) {
	// In-memory admission counts as durable for callers; the journal is only
	// a best-effort audit trail.
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.entries[digest]
	return entry, exists
}

func (s *Store) Clear(digest string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[digest]; !exists {
		return false
	}
	delete(s.entries, digest)
	s.cleared[digest] = true
	return true
}

func (s *Store) Cleared(digest string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cleared[digest]
}
