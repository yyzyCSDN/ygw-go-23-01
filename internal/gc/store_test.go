package gc

import (
	"reflect"
	"sort"
	"testing"
)

// TestCompactReturnsIndependentCopy is a regression test for an aliasing bug
// where Compact returned the internal digest cache directly. Releasing or
// adding a reference afterwards rebuilt that cache in place over the same
// backing array, which mutated a previously returned list: removing a chunk
// surfaced a duplicate of the now-last entry, and adding a chunk overwrote
// earlier entries so the list dropped or changed items. Compact must return an
// independent copy so retained lists are immune to later reference changes.
func TestCompactReturnsIndependentCopy(t *testing.T) {
	s := New()
	s.AddRef("chunk-a", 1)
	s.AddRef("chunk-b", 1)
	s.AddRef("chunk-c", 1)

	first := s.Compact()
	want := []string{"chunk-a", "chunk-b", "chunk-c"}
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("Compact = %v, want %v", first, want)
	}

	// Releasing a chunk rebuilds the internal cache. Under the bug the retained
	// list gained a duplicate (its tail reused the dropped slot's successor).
	s.ReleaseRef("chunk-b", 1)
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("Compact result mutated after ReleaseRef to %v, want %v (list must be independent of later changes)", first, want)
	}

	// Adding a chunk rebuilds the internal cache again. Under the bug the
	// retained list's earlier entries were overwritten, dropping/altering items.
	s.AddRef("chunk-d", 1)
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("Compact result mutated after AddRef to %v, want %v (list must be independent of later changes)", first, want)
	}

	// A fresh Compact must reflect the current live set, independent of the
	// retained snapshot.
	live := s.Compact()
	sort.Strings(live)
	wantLive := []string{"chunk-a", "chunk-c", "chunk-d"}
	if !reflect.DeepEqual(live, wantLive) {
		t.Fatalf("fresh Compact = %v, want %v", live, wantLive)
	}
}

// TestCompactEmptyStore covers the degenerate case so the copy path never
// returns a nil-backed slice that surprises callers.
func TestCompactEmptyStore(t *testing.T) {
	s := New()
	got := s.Compact()
	if len(got) != 0 {
		t.Fatalf("empty Compact = %v, want empty", got)
	}
}
