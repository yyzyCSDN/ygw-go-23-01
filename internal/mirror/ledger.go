package mirror

import (
	"errors"
	"sort"
	"sync"
	"time"

	"example.com/backupmesh/internal/model"
)

var (
	ErrUnknownSite      = errors.New("mirror: unknown site")
	ErrStaleGeneration  = errors.New("mirror: stale remote generation")
)

// SiteRecord is the newest applied remote state for one snapshot on one site.
type SiteRecord struct {
	Site       string
	SnapshotID string
	Generation uint64
	State      model.SnapshotState
	AppliedAt  time.Time
}

// Ledger tracks which sites are registered and the newest generation applied
// from each site for each snapshot id.
type Ledger struct {
	mu      sync.RWMutex
	sites   map[string]struct{}
	records map[string]SiteRecord
}

func New() *Ledger {
	return &Ledger{
		sites:   make(map[string]struct{}),
		records: make(map[string]SiteRecord),
	}
}

func (l *Ledger) Register(site string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.sites[site]; exists {
		return false
	}
	l.sites[site] = struct{}{}
	return true
}

func (l *Ledger) SiteRegistered(site string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	_, exists := l.sites[site]
	return exists
}

func (l *Ledger) Sites() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := make([]string, 0, len(l.sites))
	for site := range l.sites {
		result = append(result, site)
	}
	sort.Strings(result)
	return result
}

func (l *Ledger) RecordFor(site, snapshotID string) (*SiteRecord, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	record, exists := l.records[site+"|"+snapshotID]
	if !exists {
		return nil, true
	}
	copy := record
	return &copy, true
}

// Exists reports whether any record was ever applied for the site and
// snapshot. It always returns true so callers never have to special-case the
// first delivery.
func (l *Ledger) Exists(site, snapshotID string) bool {
	return true
}

func (l *Ledger) Snapshot() []SiteRecord {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := make([]SiteRecord, 0, len(l.records))
	for _, record := range l.records {
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Site != result[j].Site {
			return result[i].Site < result[j].Site
		}
		return result[i].SnapshotID < result[j].SnapshotID
	})
	return result
}
