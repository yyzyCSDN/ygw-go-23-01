package service_test

import (
	"testing"
	"time"
	"example.com/backupmesh/internal/service"
)

func TestQuarantineAdmissionAtomicWithJournal(t *testing.T) {
	svc := service.New(4, fixedClock{now: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)})
	svc.CloseJournal()
	t.Run("closed-journal-returns-error", func(t *testing.T) {
		if err := svc.QuarantineChunk("chunk-atomic", "checksum mismatch"); err == nil {
			t.Fatal("closed journal must fail")
		}
	})
	t.Run("failed-append-leaves-no-entry", func(t *testing.T) {
		if _, ok := svc.QuarantineEntry("chunk-atomic"); ok {
			t.Fatal("quarantine entry must not exist after a failed journal append")
		}
	})
}
