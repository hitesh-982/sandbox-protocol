package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/samsung/agentic-sandbox/pkg/api"
)

var (
	ErrNotFound      = errors.New("record not found")
	ErrAlreadyExists = errors.New("record already exists")
)

// SandboxRecord stores metadata for a sandbox.
type SandboxRecord struct {
	SandboxID  string           `json:"sandbox_id"`
	ThreadID   string           `json:"thread_id"`
	HostID     string           `json:"host_id"`
	AgentAddr  string           `json:"agent_addr"` // address to reach the in-pod agent
	State      api.SandboxState `json:"state"`
	Image      string           `json:"image"`
	LastActive time.Time        `json:"last_active"`
	CreatedAt  time.Time        `json:"created_at"`
}

// Store defines the interface for state management with CAS semantics.
type Store interface {
	// BindThread attempts to atomically bind threadID -> sandboxID if unbound.
	// If threadID is already bound, returns (existingSandboxID, won=false, nil).
	// If successfully bound, returns (sandboxID, won=true, nil).
	BindThread(ctx context.Context, threadID, sandboxID string) (actualSandboxID string, won bool, err error)

	// GetThreadBinding retrieves the sandboxID bound to threadID.
	GetThreadBinding(ctx context.Context, threadID string) (string, error)

	// UnbindThread removes thread binding (used during teardown).
	UnbindThread(ctx context.Context, threadID string) error

	// SetSandbox saves or updates a sandbox record.
	SetSandbox(ctx context.Context, record *SandboxRecord) error

	// GetSandbox retrieves a sandbox record by ID.
	GetSandbox(ctx context.Context, sandboxID string) (*SandboxRecord, error)

	// DeleteSandbox deletes a sandbox record and its leases.
	DeleteSandbox(ctx context.Context, sandboxID string) error

	// ListSandboxes returns all sandboxes, optionally filtered by state.
	ListSandboxes(ctx context.Context, state api.SandboxState) ([]*SandboxRecord, error)

	// RefreshIdleLease sets/refreshes the idle TTL lease for sandboxID.
	RefreshIdleLease(ctx context.Context, sandboxID string, ttl time.Duration) error

	// IsIdleLeaseExpired returns true if the idle lease is expired or missing.
	IsIdleLeaseExpired(ctx context.Context, sandboxID string) (bool, error)

	// DeleteIdleLease removes the idle lease.
	DeleteIdleLease(ctx context.Context, sandboxID string) error
}

type memoryLease struct {
	expiresAt time.Time
}

// MemoryStore provides an in-memory implementation of Store with CAS and lease semantics.
type MemoryStore struct {
	mu        sync.RWMutex
	threads   map[string]string         // /threads/{thread_id} -> sandbox_id
	sandboxes map[string]*SandboxRecord // /sandboxes/{sandbox_id} -> record
	leases    map[string]memoryLease    // /leases/idle/{sandbox_id} -> lease
}

// NewMemoryStore initializes a new MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		threads:   make(map[string]string),
		sandboxes: make(map[string]*SandboxRecord),
		leases:    make(map[string]memoryLease),
	}
}

func (m *MemoryStore) BindThread(ctx context.Context, threadID, sandboxID string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.threads[threadID]; ok && existing != "" {
		return existing, false, nil
	}
	m.threads[threadID] = sandboxID
	return sandboxID, true, nil
}

func (m *MemoryStore) GetThreadBinding(ctx context.Context, threadID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if val, ok := m.threads[threadID]; ok && val != "" {
		return val, nil
	}
	return "", ErrNotFound
}

func (m *MemoryStore) UnbindThread(ctx context.Context, threadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.threads, threadID)
	return nil
}

func (m *MemoryStore) SetSandbox(ctx context.Context, record *SandboxRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clone record to prevent race conditions on shared pointers
	copied := *record
	m.sandboxes[record.SandboxID] = &copied
	return nil
}

func (m *MemoryStore) GetSandbox(ctx context.Context, sandboxID string) (*SandboxRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	record, ok := m.sandboxes[sandboxID]
	if !ok {
		return nil, ErrNotFound
	}
	copied := *record
	return &copied, nil
}

func (m *MemoryStore) DeleteSandbox(ctx context.Context, sandboxID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if rec, ok := m.sandboxes[sandboxID]; ok {
		delete(m.threads, rec.ThreadID)
	}
	delete(m.sandboxes, sandboxID)
	delete(m.leases, sandboxID)
	return nil
}

func (m *MemoryStore) ListSandboxes(ctx context.Context, state api.SandboxState) ([]*SandboxRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*SandboxRecord
	for _, rec := range m.sandboxes {
		if state == "" || rec.State == state {
			copied := *rec
			result = append(result, &copied)
		}
	}
	return result, nil
}

func (m *MemoryStore) RefreshIdleLease(ctx context.Context, sandboxID string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.leases[sandboxID] = memoryLease{
		expiresAt: time.Now().Add(ttl),
	}
	return nil
}

func (m *MemoryStore) IsIdleLeaseExpired(ctx context.Context, sandboxID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	lease, ok := m.leases[sandboxID]
	if !ok {
		return true, nil
	}
	return time.Now().After(lease.expiresAt), nil
}

func (m *MemoryStore) DeleteIdleLease(ctx context.Context, sandboxID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.leases, sandboxID)
	return nil
}
