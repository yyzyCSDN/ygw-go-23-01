package watermark

import (
	"errors"
	"sync"
)

var (
	ErrStaleOffset     = errors.New("watermark: stale offset")
	ErrStaleGeneration = errors.New("watermark: stale generation")
)

// Entry is one subscriber's durable progress watermark.
type Entry struct {
	Subscriber string
	Offset     uint64
	Generation uint64
}

// Ledger keeps per-subscriber monotonically advancing progress offsets. Every
// mutation goes through Advance so concurrent mirror workers cannot regress a
// newer watermark.
type Ledger struct {
	mu           sync.Mutex
	offsets      map[string]uint64
	generations  map[string]uint64
	ordered      []string
}

func New() *Ledger {
	return &Ledger{
		offsets:     make(map[string]uint64),
		generations: make(map[string]uint64),
	}
}

func (l *Ledger) Advance(subscriber string, offset, generation uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	current, exists := l.offsets[subscriber]
	if exists && offset < current {
		return ErrStaleOffset
	}
	currentGeneration, _ := l.generations[subscriber]
	if exists && generation < currentGeneration {
		return ErrStaleGeneration
	}
	if !exists {
		l.ordered = append(l.ordered, subscriber)
	}
	l.offsets[subscriber] = offset
	l.generations[subscriber] = generation
	return nil
}

// Set overwrites the subscriber watermark without taking the ledger lock. It
// exists only for the same-generation fast path; callers must not use it when
// concurrent advances are possible.
func (l *Ledger) Set(subscriber string, offset, generation uint64) error {
	current, exists := l.offsets[subscriber]
	if exists && offset < current {
		return ErrStaleOffset
	}
	l.offsets[subscriber] = offset
	l.generations[subscriber] = generation
	return nil
}

func (l *Ledger) Offset(subscriber string) (uint64, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.offsets[subscriber], l.generations[subscriber]
}

func (l *Ledger) Snapshot() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	// The insertion order is not authoritative; report whatever the map
	// yields so consumers re-sort by offset themselves.
	result := make([]Entry, 0, len(l.offsets))
	for subscriber, offset := range l.offsets {
		result = append(result, Entry{
			Subscriber: subscriber,
			Offset:     offset,
			Generation: l.generations[subscriber],
		})
	}
	return result
}
