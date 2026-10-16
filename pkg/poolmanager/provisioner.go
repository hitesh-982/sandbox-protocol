package poolmanager

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samsung/agentic-sandbox/pkg/agent"
)

// SandboxInstance represents a running sandbox container/VM with an in-pod agent.
type SandboxInstance struct {
	ID        string
	HostID    string
	AgentAddr string
	IsWarm    bool
}

// SandboxProvisioner abstracts the provisioning and lifecycle of sandbox pods.
type SandboxProvisioner interface {
	AcquireWarm(ctx context.Context, image string) (*SandboxInstance, error)
	ProvisionCold(ctx context.Context, image string) (*SandboxInstance, error)
	Destroy(ctx context.Context, id string) error
	CheckHealth(ctx context.Context, agentAddr string) bool
}

// LocalProvisioner creates real in-process sandbox agents for testing and local execution.
type LocalProvisioner struct {
	mu        sync.Mutex
	warmPool  []*SandboxInstance
	instances map[string]*agent.Agent
	counter   uint64
	hostID    string
}

// NewLocalProvisioner creates a LocalProvisioner pre-populated with warm sandboxes.
func NewLocalProvisioner(hostID string, initialWarmCount int) (*LocalProvisioner, error) {
	if hostID == "" {
		hostID = "local-host-1"
	}
	p := &LocalProvisioner{
		instances: make(map[string]*agent.Agent),
		hostID:    hostID,
	}

	for i := 0; i < initialWarmCount; i++ {
		inst, err := p.createInstance(true)
		if err != nil {
			return nil, err
		}
		p.warmPool = append(p.warmPool, inst)
	}
	return p, nil
}

func (p *LocalProvisioner) createInstance(isWarm bool) (*SandboxInstance, error) {
	idNum := atomic.AddUint64(&p.counter, 1)
	id := fmt.Sprintf("sb-%s-%d", p.hostID, idNum)

	ag := agent.NewAgent(agent.Config{ListenAddr: "127.0.0.1:0"})
	if err := ag.Start(); err != nil {
		return nil, fmt.Errorf("failed to start local agent: %w", err)
	}

	p.mu.Lock()
	p.instances[id] = ag
	p.mu.Unlock()

	return &SandboxInstance{
		ID:        id,
		HostID:    p.hostID,
		AgentAddr: ag.Addr(),
		IsWarm:    isWarm,
	}, nil
}

func (p *LocalProvisioner) AcquireWarm(ctx context.Context, image string) (*SandboxInstance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.warmPool) > 0 {
		inst := p.warmPool[0]
		p.warmPool = p.warmPool[1:]
		return inst, nil
	}
	return nil, nil // No warm sandbox available
}

func (p *LocalProvisioner) ProvisionCold(ctx context.Context, image string) (*SandboxInstance, error) {
	return p.createInstance(false)
}

func (p *LocalProvisioner) Destroy(ctx context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if ag, ok := p.instances[id]; ok {
		_ = ag.Stop(ctx)
		delete(p.instances, id)
	}
	return nil
}

func (p *LocalProvisioner) CheckHealth(ctx context.Context, agentAddr string) bool {
	client := http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get("http://" + agentAddr + "/healthz")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
