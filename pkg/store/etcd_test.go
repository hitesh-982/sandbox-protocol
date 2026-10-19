package store

import (
	"context"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/samsung/agentic-sandbox/pkg/api"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func startEmbeddedEtcd(t *testing.T) (*clientv3.Client, func()) {
	dir, err := os.MkdirTemp("", "etcd-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.LogLevel = "error"

	lcurl, _ := url.Parse("http://127.0.0.1:0")
	lpurl, _ := url.Parse("http://127.0.0.1:0")
	cfg.ListenClientUrls = []url.URL{*lcurl}
	cfg.ListenPeerUrls = []url.URL{*lpurl}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to start embedded etcd: %v", err)
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		e.Close()
		os.RemoveAll(dir)
		t.Fatalf("embedded etcd took too long to start")
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{e.Clients[0].Addr().String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		e.Close()
		os.RemoveAll(dir)
		t.Fatalf("failed to create client: %v", err)
	}

	cleanup := func() {
		_ = client.Close()
		e.Close()
		_ = os.RemoveAll(dir)
	}

	return client, cleanup
}

func TestEtcdStore_CASThreadBinding(t *testing.T) {
	client, cleanup := startEmbeddedEtcd(t)
	defer cleanup()

	s := NewEtcdStoreFromClient(client, "/test-prefix")
	ctx := context.Background()

	threadID := "thread-etcd-1"
	winnerID := "sb-winner"

	// First bind succeeds
	actual, won, err := s.BindThread(ctx, threadID, winnerID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !won || actual != winnerID {
		t.Fatalf("expected first bind to win, got actual=%s won=%v", actual, won)
	}

	// Second bind fails CAS and returns winner
	loserID := "sb-loser"
	actual2, won2, err := s.BindThread(ctx, threadID, loserID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if won2 || actual2 != winnerID {
		t.Fatalf("expected second bind to lose, got actual=%s won=%v", actual2, won2)
	}
}

func TestEtcdStore_ConcurrentRacingCreates(t *testing.T) {
	client, cleanup := startEmbeddedEtcd(t)
	defer cleanup()

	s := NewEtcdStoreFromClient(client, "/test-prefix")
	ctx := context.Background()

	threadID := "thread-etcd-race"
	concurrency := 20
	var wg sync.WaitGroup
	winners := make([]string, concurrency)
	wons := make([]bool, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sbID := "sb-race-" + string(rune('a'+idx))
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
		t.Fatalf("expected exactly 1 winner in etcd CAS race, got %d", winCount)
	}

	for i := 0; i < concurrency; i++ {
		if winners[i] != singleWinner {
			t.Fatalf("goroutine %d got mismatched winner: %s != %s", i, winners[i], singleWinner)
		}
	}
}

func TestEtcdStore_SandboxCRUD(t *testing.T) {
	client, cleanup := startEmbeddedEtcd(t)
	defer cleanup()

	s := NewEtcdStoreFromClient(client, "/test-prefix")
	ctx := context.Background()

	rec := &SandboxRecord{
		SandboxID:  "sb-etcd-001",
		ThreadID:   "thread-crud",
		HostID:     "node-1",
		AgentAddr:  "127.0.0.1:9999",
		State:      api.StateBound,
		Image:      "samsung/kata:v1",
		LastActive: time.Now().UTC().Truncate(time.Millisecond),
		CreatedAt:  time.Now().UTC().Truncate(time.Millisecond),
	}

	if err := s.SetSandbox(ctx, rec); err != nil {
		t.Fatalf("failed to set sandbox: %v", err)
	}

	got, err := s.GetSandbox(ctx, "sb-etcd-001")
	if err != nil {
		t.Fatalf("failed to get sandbox: %v", err)
	}
	if got.State != api.StateBound || got.ThreadID != "thread-crud" {
		t.Fatalf("unexpected record: %+v", got)
	}

	list, err := s.ListSandboxes(ctx, api.StateBound)
	if err != nil || len(list) != 1 {
		t.Fatalf("expected 1 sandbox in list, got %d (err: %v)", len(list), err)
	}

	if err := s.DeleteSandbox(ctx, "sb-etcd-001"); err != nil {
		t.Fatalf("failed to delete sandbox: %v", err)
	}

	_, err = s.GetSandbox(ctx, "sb-etcd-001")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestEtcdStore_IdleLeaseTTL(t *testing.T) {
	client, cleanup := startEmbeddedEtcd(t)
	defer cleanup()

	s := NewEtcdStoreFromClient(client, "/test-prefix")
	ctx := context.Background()

	sbID := "sb-lease-etcd"

	// Missing lease is expired
	expired, err := s.IsIdleLeaseExpired(ctx, sbID)
	if err != nil || !expired {
		t.Fatalf("expected missing lease to be expired, got %v", expired)
	}

	// Grant 1s lease
	err = s.RefreshIdleLease(ctx, sbID, 1*time.Second)
	if err != nil {
		t.Fatalf("failed to refresh idle lease: %v", err)
	}

	expired, err = s.IsIdleLeaseExpired(ctx, sbID)
	if err != nil || expired {
		t.Fatalf("expected active lease, got expired=%v", expired)
	}

	// Poll until lease is expired (up to 3.5 seconds for etcd lessor revoke sweep)
	expired = false
	deadline := time.Now().Add(3500 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		expired, err = s.IsIdleLeaseExpired(ctx, sbID)
		if err == nil && expired {
			break
		}
	}

	if !expired {
		t.Fatalf("expected lease to expire after TTL, got expired=%v", expired)
	}
}
