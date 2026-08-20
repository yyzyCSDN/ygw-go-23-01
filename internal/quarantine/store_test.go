package quarantine_test

import (
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/quarantine"
)

var verifierErr = errors.New("verifier unavailable")

func TestReverifyMatchMarksVerified(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	store := quarantine.New()
	if err := store.Admit("chunk-a", "checksum mismatch", now); err != nil {
		t.Fatal(err)
	}

	if err := store.Reverify("chunk-a", true, nil, now); err != nil {
		t.Fatalf("reverify returned error: %v", err)
	}

	entry, ok := store.Entry("chunk-a")
	if !ok {
		t.Fatal("entry should still exist after successful re-verification")
	}
	if !entry.Verified {
		t.Fatalf("entry should be verified after a real match, got %+v", entry)
	}
}

func TestReverifyErrorPropagatedAndQuarantineKept(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	store := quarantine.New()
	if err := store.Admit("chunk-err", "checksum mismatch", now); err != nil {
		t.Fatal(err)
	}

	// A verification infrastructure error must be surfaced, never treated as a
	// pass. This is the regression for the swallowed-error bug: even when the
	// caller reports matches=true, an error must keep the chunk unverified and
	// the quarantine in effect.
	err := store.Reverify("chunk-err", true, verifierErr, now)
	if !errors.Is(err, verifierErr) {
		t.Fatalf("reverify should propagate the verification error, got %v", err)
	}

	entry, ok := store.Entry("chunk-err")
	if !ok {
		t.Fatal("quarantine entry must remain after a verification error")
	}
	if entry.Verified {
		t.Fatalf("chunk must not be marked verified on a verification error, got %+v", entry)
	}

	// The quarantine must still be clearable later, proving it was never
	// released by the failed re-verification.
	if store.Cleared("chunk-err") {
		t.Fatal("chunk must not be cleared on a verification error")
	}
}

func TestReverifyErrorWithMismatchAlsoPropagated(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	store := quarantine.New()
	if err := store.Admit("chunk-err2", "checksum mismatch", now); err != nil {
		t.Fatal(err)
	}

	err := store.Reverify("chunk-err2", false, verifierErr, now)
	if !errors.Is(err, verifierErr) {
		t.Fatalf("reverify should propagate the verification error even on mismatch, got %v", err)
	}

	entry, ok := store.Entry("chunk-err2")
	if !ok {
		t.Fatal("quarantine entry must remain after a verification error")
	}
	if entry.Verified {
		t.Fatalf("chunk must not be marked verified on a verification error, got %+v", entry)
	}
}

func TestReverifyMismatchKeepsQuarantineUnverified(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	store := quarantine.New()
	if err := store.Admit("chunk-mismatch", "checksum mismatch", now); err != nil {
		t.Fatal(err)
	}

	// A clean mismatch (no infra error) is a definitive result, not a pass:
	// the quarantine stays in effect and the chunk is left unverified.
	if err := store.Reverify("chunk-mismatch", false, nil, now); err != nil {
		t.Fatalf("reverify should not error on a clean mismatch, got %v", err)
	}

	entry, ok := store.Entry("chunk-mismatch")
	if !ok {
		t.Fatal("quarantine entry must remain after a mismatch")
	}
	if entry.Verified {
		t.Fatalf("chunk must not be marked verified on a mismatch, got %+v", entry)
	}
	if entry.Reason != "digest mismatch after re-verification" {
		t.Fatalf("reason should be updated to mismatch, got %q", entry.Reason)
	}
}

func TestReverifyNotQuarantinedReturnsError(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	store := quarantine.New()

	if err := store.Reverify("missing", true, nil, now); !errors.Is(err, quarantine.ErrNotQuarantined) {
		t.Fatalf("reverify of unknown digest should return ErrNotQuarantined, got %v", err)
	}
}

func TestReverifyErrorDoesNotReleaseLaterSuccess(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	store := quarantine.New()
	if err := store.Admit("chunk-retry", "checksum mismatch", now); err != nil {
		t.Fatal(err)
	}

	// An error must not release the chunk; a later, genuinely successful
	// comparison is still required and still works.
	if err := store.Reverify("chunk-retry", true, verifierErr, now); !errors.Is(err, verifierErr) {
		t.Fatalf("first reverify should propagate error, got %v", err)
	}
	entry, _ := store.Entry("chunk-retry")
	if entry.Verified {
		t.Fatal("chunk must not be verified after an error")
	}

	if err := store.Reverify("chunk-retry", true, nil, now); err != nil {
		t.Fatalf("later successful reverify should succeed, got %v", err)
	}
	entry, _ = store.Entry("chunk-retry")
	if !entry.Verified {
		t.Fatal("chunk should be verified after a real successful comparison")
	}
}
