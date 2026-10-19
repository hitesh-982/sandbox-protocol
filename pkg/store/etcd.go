package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/samsung/agentic-sandbox/pkg/api"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdStore implements the Store interface using etcd v3.
type EtcdStore struct {
	client    *clientv3.Client
	keyPrefix string
}

// EtcdConfig holds configuration for the EtcdStore.
type EtcdConfig struct {
	Endpoints   []string
	DialTimeout time.Duration
	KeyPrefix   string // Optional prefix to isolate keys (e.g. "/samsung/sandbox")
	Username    string
	Password    string
}

// NewEtcdStore creates a new Store backed by etcd v3.
func NewEtcdStore(cfg EtcdConfig) (*EtcdStore, error) {
	if len(cfg.Endpoints) == 0 {
		cfg.Endpoints = []string{"127.0.0.1:2379"}
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
		Username:    cfg.Username,
		Password:    cfg.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	prefix := strings.TrimSuffix(cfg.KeyPrefix, "/")
	return &EtcdStore{
		client:    client,
		keyPrefix: prefix,
	}, nil
}

// NewEtcdStoreFromClient wraps an existing etcd v3 client.
func NewEtcdStoreFromClient(client *clientv3.Client, keyPrefix string) *EtcdStore {
	return &EtcdStore{
		client:    client,
		keyPrefix: strings.TrimSuffix(keyPrefix, "/"),
	}
}

// Close closes the underlying etcd client connection.
func (s *EtcdStore) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func (s *EtcdStore) threadKey(threadID string) string {
	return s.keyPrefix + "/threads/" + threadID
}

func (s *EtcdStore) sandboxKey(sandboxID string) string {
	return s.keyPrefix + "/sandboxes/" + sandboxID
}

func (s *EtcdStore) leaseKey(sandboxID string) string {
	return s.keyPrefix + "/leases/idle/" + sandboxID
}

// BindThread executes an atomic Compare-And-Swap transaction on /threads/{thread_id}.
// If the key does not exist (version == 0), it sets it to sandboxID and returns (sandboxID, true, nil).
// If it already exists, the transaction fails, and it returns (existingID, false, nil).
func (s *EtcdStore) BindThread(ctx context.Context, threadID, sandboxID string) (string, bool, error) {
	key := s.threadKey(threadID)

	txnResp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, sandboxID)).
		Else(clientv3.OpGet(key)).
		Commit()

	if err != nil {
		return "", false, fmt.Errorf("etcd CAS txn failed: %w", err)
	}

	if txnResp.Succeeded {
		// Winner: successfully bound thread_id -> sandboxID
		return sandboxID, true, nil
	}

	// Loser: retrieve the winning sandbox_id returned by the Else OpGet
	if len(txnResp.Responses) > 0 {
		getResp := txnResp.Responses[0].GetResponseRange()
		if getResp != nil && len(getResp.Kvs) > 0 {
			existingID := string(getResp.Kvs[0].Value)
			return existingID, false, nil
		}
	}

	// Fallback lookup if not in txn response
	existing, err := s.GetThreadBinding(ctx, threadID)
	if err != nil {
		return "", false, err
	}
	return existing, false, nil
}

// GetThreadBinding retrieves the sandbox ID mapped to threadID.
func (s *EtcdStore) GetThreadBinding(ctx context.Context, threadID string) (string, error) {
	key := s.threadKey(threadID)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return "", fmt.Errorf("failed to get thread binding: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return "", ErrNotFound
	}
	return string(resp.Kvs[0].Value), nil
}

// UnbindThread removes the thread binding.
func (s *EtcdStore) UnbindThread(ctx context.Context, threadID string) error {
	key := s.threadKey(threadID)
	_, err := s.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to unbind thread: %w", err)
	}
	return nil
}

// SetSandbox persists a SandboxRecord as JSON in /sandboxes/{sandbox_id}.
func (s *EtcdStore) SetSandbox(ctx context.Context, record *SandboxRecord) error {
	key := s.sandboxKey(record.SandboxID)
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal sandbox record: %w", err)
	}

	_, err = s.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to put sandbox record: %w", err)
	}
	return nil
}

// GetSandbox retrieves and unmarshals a SandboxRecord from /sandboxes/{sandbox_id}.
func (s *EtcdStore) GetSandbox(ctx context.Context, sandboxID string) (*SandboxRecord, error) {
	key := s.sandboxKey(sandboxID)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get sandbox record: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return nil, ErrNotFound
	}

	var record SandboxRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sandbox record: %w", err)
	}
	return &record, nil
}

// DeleteSandbox deletes the sandbox record, thread binding, and idle lease.
func (s *EtcdStore) DeleteSandbox(ctx context.Context, sandboxID string) error {
	rec, err := s.GetSandbox(ctx, sandboxID)
	if err == nil && rec != nil && rec.ThreadID != "" {
		_ = s.UnbindThread(ctx, rec.ThreadID)
	}

	key := s.sandboxKey(sandboxID)
	_, err = s.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete sandbox record: %w", err)
	}

	_ = s.DeleteIdleLease(ctx, sandboxID)
	return nil
}

// ListSandboxes lists all sandboxes matching an optional state filter.
func (s *EtcdStore) ListSandboxes(ctx context.Context, state api.SandboxState) ([]*SandboxRecord, error) {
	prefix := s.keyPrefix + "/sandboxes/"
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list sandboxes: %w", err)
	}

	var results []*SandboxRecord
	for _, kv := range resp.Kvs {
		var record SandboxRecord
		if err := json.Unmarshal(kv.Value, &record); err != nil {
			continue
		}
		if state == "" || record.State == state {
			results = append(results, &record)
		}
	}
	return results, nil
}

// RefreshIdleLease grants an etcd lease with ttl and puts /leases/idle/{sandbox_id} attached to it.
func (s *EtcdStore) RefreshIdleLease(ctx context.Context, sandboxID string, ttl time.Duration) error {
	ttlSeconds := int64(ttl.Seconds())
	if ttlSeconds <= 0 {
		ttlSeconds = 1
	}

	leaseResp, err := s.client.Grant(ctx, ttlSeconds)
	if err != nil {
		return fmt.Errorf("failed to grant lease: %w", err)
	}

	key := s.leaseKey(sandboxID)
	_, err = s.client.Put(ctx, key, "", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return fmt.Errorf("failed to put lease key: %w", err)
	}
	return nil
}

// IsIdleLeaseExpired checks if /leases/idle/{sandbox_id} exists.
// When an etcd lease expires, etcd automatically deletes the key.
func (s *EtcdStore) IsIdleLeaseExpired(ctx context.Context, sandboxID string) (bool, error) {
	key := s.leaseKey(sandboxID)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return true, fmt.Errorf("failed to check idle lease: %w", err)
	}
	return len(resp.Kvs) == 0, nil
}

// DeleteIdleLease deletes the lease key immediately.
func (s *EtcdStore) DeleteIdleLease(ctx context.Context, sandboxID string) error {
	key := s.leaseKey(sandboxID)
	_, err := s.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete idle lease: %w", err)
	}
	return nil
}
