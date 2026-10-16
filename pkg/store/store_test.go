package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/samsung/agentic-sandbox/pkg/api"
)

func TestStore_CASThreadBinding(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	threadID := "thread-123"
	winnerID := "sb-winner"

	actual, won, err := s.BindThread(ctx, threadID, winnerID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !won || actual != winnerID {
		t.Fatalf("expected first bind to win, got actual=%s won=%v", actual, won)
	}

	// Second bind must lose and return winnerID
	loserID := "sb-loser"
	actual2, won2, err := s.BindThread(ctx, threadID, loserID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if won2 || actual2 != winnerID {
		t.Fatalf("expected second bind to lose with winnerID, got actual=%s won=%v", actual2, won2)
	}
}

func TestStore_ConcurrentRacingCreates(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	threadID := "thread-race"
	concurrency := 20
	var wg sync.WaitGroup
	winners := make([]string, concurrency)
	wons := make([]bool, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sbID := "sb-" + string(rune('a'+idx))
			actual, won, _ := s.BindThread(ctx, threadID, sbID)
			winners[idx] = actual
			wons[idx] = won
		}(i)
	}
	wg.Wait()

	winCount := 0
	var singleWinner string
	for i := 0; i < concurrency; i++ {
		if wons[i] {
			winCount++
			singleWinner = winners[i]
		}
	}

	if winCount != 1 {
		t.Fatalf("expected exactly 1 winner in concurrent CAS race, got %d", winCount)
	}

	// Verify all losers received the same winner ID
	for i := 0; i < concurrency; i++ {
		if winners[i] != singleWinner {
			t.Fatalf("goroutine %d got mismatched winner: %s != %s", i, winners[i], singleWinner)
		}
	}
}

func TestStore_IdleLease(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	sbID := "sb-lease-test"
	// Lease not set yet -> expired
	expired, err := s.IsIdleLeaseExpired(ctx, sbID)
	if err != nil || !expired {
		t.Fatalf("expected missing lease to be expired, got %v, err=%v", expired, err)
	}

	// Set lease with short TTL
	err = s.RefreshIdleLease(ctx, sbID, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expired, err = s.IsIdleLeaseExpired(ctx, sbID)
	if err != nil || expired {
		t.Fatalf("expected active lease to not be expired, got %v", expired)
	}

	time.Sleep(70 * time.Millisecond)
	expired, err = s.IsIdleLeaseExpired(ctx, sbID)
	if err != nil || !expired {
		t.Fatalf("expected lease to expire after sleep, got %v", expired)
	}
}

func TestStore_SandboxCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	rec := &SandboxRecord{
		SandboxID:  "sb-001",
		ThreadID:   "thread-abc",
		HostID:     "host-node-1",
		State:      api.StateBound,
		Image:      "samsung/sandbox:v1",
		LastActive: time.Now(),
		CreatedAt:  time.Now(),
	}

	if err := s.SetSandbox(ctx, rec); err != nil {
		t.Fatalf("failed to set sandbox: %v", err)
	}

	got, err := s.GetSandbox(ctx, "sb-001")
	if err != nil {
		t.Fatalf("failed to get sandbox: %v", err)
	}
	if got.State != api.StateBound || got.ThreadID != "thread-abc" {
		t.Fatalf("unexpected record: %+v", got)
	}

	if err := s.DeleteSandbox(ctx, "sb-001"); err != nil {
		t.Fatalf("failed to delete sandbox: %v", err)
	}

	_, err = s.GetSandbox(ctx, "sb-001")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}
