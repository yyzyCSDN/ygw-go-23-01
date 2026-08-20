package service_test

import (
	"errors"
	"testing"
	"time"
	"example.com/backupmesh/internal/service"
)

func TestQuarantineReverifyPropagatesError(t *testing.T) {
	svc := service.New(4, fixedClock{now: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)})
	if err := svc.QuarantineChunk("chunk-q", "checksum mismatch"); err != nil {
		t.Fatal(err)
	}
	t.Run("error-propagates", func(t *testing.T) {
		verifyErr := errors.New("digest verification unavailable")
		if err := svc.ReverifyChunk("chunk-q", false, verifyErr); err == nil {
			t.Fatal("verify error must propagate")
		}
	})
	t.Run("stays-quarantined-after-error", func(t *testing.T) {
		entry, ok := svc.QuarantineEntry("chunk-q")
		if !ok || entry.Verified {
			t.Fatalf("entry must stay active after a failed verification: %+v ok=%v", entry, ok)
		}
	})
	t.Run("verified-only-after-match", func(t *testing.T) {
		if err := svc.ReverifyChunk("chunk-q", true, nil); err != nil {
			t.Fatal(err)
		}
		entry, _ := svc.QuarantineEntry("chunk-q")
		if !entry.Verified {
			t.Fatal("successful comparison must clear the quarantine")
		}
	})
}
