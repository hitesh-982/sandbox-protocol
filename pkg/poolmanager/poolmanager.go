package poolmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/samsung/agentic-sandbox/pkg/api"
	"github.com/samsung/agentic-sandbox/pkg/store"
)

// Config configures the Pool Manager service.
type Config struct {
	ListenAddr       string
	IdleTTL          time.Duration
	DeleteAfterStop  time.Duration
	ReaperInterval   time.Duration
}

// PoolManager implements the stateless HTTP control plane service for sandboxes.
type PoolManager struct {
	cfg         Config
	store       store.Store
	provisioner SandboxProvisioner
	httpServer  *http.Server
	listener    net.Listener
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// New creates a new PoolManager instance.
func New(cfg Config, s store.Store, prov SandboxProvisioner) *PoolManager {
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = 10 * time.Minute
	}
	if cfg.DeleteAfterStop <= 0 {
		cfg.DeleteAfterStop = 60 * time.Minute
	}
	if cfg.ReaperInterval <= 0 {
		cfg.ReaperInterval = 30 * time.Second
	}

	pm := &PoolManager{
		cfg:         cfg,
		store:       s,
		provisioner: prov,
		stopCh:      make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/sandboxes", pm.handleSandboxesRoot)
	mux.HandleFunc("/v1/sandboxes/", pm.handleSandboxByID)
	mux.HandleFunc("/healthz", pm.handleHealthz)

	pm.httpServer = &http.Server{Handler: mux}
	return pm
}

// Start launches the PoolManager HTTP listener and background reaper loop.
func (pm *PoolManager) Start() error {
	addr := pm.cfg.ListenAddr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	pm.listener = ln

	pm.wg.Add(2)
	go func() {
		defer pm.wg.Done()
		_ = pm.httpServer.Serve(ln)
	}()

	go func() {
		defer pm.wg.Done()
		pm.runReaper()
	}()

	return nil
}

// Addr returns the listening network address.
func (pm *PoolManager) Addr() string {
	if pm.listener == nil {
		return pm.cfg.ListenAddr
	}
	return pm.listener.Addr().String()
}

// Stop cleanly terminates the PoolManager and waits for workers to finish.
func (pm *PoolManager) Stop(ctx context.Context) error {
	close(pm.stopCh)
	if pm.httpServer != nil {
		_ = pm.httpServer.Shutdown(ctx)
	}
	pm.wg.Wait()
	return nil
}

func (pm *PoolManager) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK\n"))
}

// handleSandboxesRoot routes POST /v1/sandboxes.
func (pm *PoolManager) handleSandboxesRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		pm.handleAcquire(w, r)
		return
	}
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

// handleAcquire handles POST /v1/sandboxes (idempotent on thread_id).
func (pm *PoolManager) handleAcquire(w http.ResponseWriter, r *http.Request) {
	var req api.AcquireRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid acquire request: %v", err), http.StatusBadRequest)
		return
	}

	if req.ThreadID == "" {
		http.Error(w, "thread_id is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// 1. Check if thread_id is already bound (Warm Reconnect check)
	existingID, err := pm.store.GetThreadBinding(ctx, req.ThreadID)
	if err == nil && existingID != "" {
		existingRec, err := pm.store.GetSandbox(ctx, existingID)
		if err == nil && existingRec != nil {
			// Health check the existing sandbox
			if pm.provisioner.CheckHealth(ctx, existingRec.AgentAddr) {
				// Reconnect success!
				now := time.Now()
				existingRec.LastActive = now
				existingRec.State = api.StateBound
				_ = pm.store.SetSandbox(ctx, existingRec)
				_ = pm.store.RefreshIdleLease(ctx, existingID, pm.cfg.IdleTTL)

				resp := api.AcquireResponse{
					SandboxID: existingRec.SandboxID,
					HostID:    existingRec.HostID,
					Path:      api.PathReconnect,
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
			// Unhealthy: mark unreachable and allocate replacement
			existingRec.State = api.StateUnreachable
			_ = pm.store.SetSandbox(ctx, existingRec)
			_ = pm.store.UnbindThread(ctx, req.ThreadID)
		}
	}

	// 2. Allocate new sandbox (Warm Allocation or Cold Boot)
	var inst *SandboxInstance
	var path api.AcquirePath

	// Try warm pool
	inst, err = pm.provisioner.AcquireWarm(ctx, req.Image)
	if err == nil && inst != nil {
		path = api.PathWarm
	} else {
		// Cold boot
		inst, err = pm.provisioner.ProvisionCold(ctx, req.Image)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to provision sandbox: %v", err), http.StatusInternalServerError)
			return
		}
		path = api.PathCold
	}

	// 3. Atomically CAS bind thread_id -> inst.ID
	actualID, won, err := pm.store.BindThread(ctx, req.ThreadID, inst.ID)
	if err != nil {
		_ = pm.provisioner.Destroy(ctx, inst.ID)
		http.Error(w, fmt.Sprintf("state store error: %v", err), http.StatusInternalServerError)
		return
	}

	// 4. Handle CAS conflict (another racing create won)
	if !won {
		// Loser destroys its candidate sandbox and returns winner's binding
		_ = pm.provisioner.Destroy(ctx, inst.ID)
		winnerRec, err := pm.store.GetSandbox(ctx, actualID)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to retrieve winning sandbox: %v", err), http.StatusInternalServerError)
			return
		}
		resp := api.AcquireResponse{
			SandboxID: winnerRec.SandboxID,
			HostID:    winnerRec.HostID,
			Path:      api.PathReconnect,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// 5. Winner persists its sandbox record and registers idle lease
	now := time.Now()
	rec := &store.SandboxRecord{
		SandboxID:  inst.ID,
		ThreadID:   req.ThreadID,
		HostID:     inst.HostID,
		AgentAddr:  inst.AgentAddr,
		State:      api.StateBound,
		Image:      req.Image,
		LastActive: now,
		CreatedAt:  now,
	}
	if err := pm.store.SetSandbox(ctx, rec); err != nil {
		_ = pm.provisioner.Destroy(ctx, inst.ID)
		http.Error(w, fmt.Sprintf("failed to record sandbox: %v", err), http.StatusInternalServerError)
		return
	}
	_ = pm.store.RefreshIdleLease(ctx, inst.ID, pm.cfg.IdleTTL)

	resp := api.AcquireResponse{
		SandboxID: inst.ID,
		HostID:    inst.HostID,
		Path:      path,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleSandboxByID routes /v1/sandboxes/{id} and /v1/sandboxes/{id}/exec.
func (pm *PoolManager) handleSandboxByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/sandboxes/")
	parts := strings.Split(path, "/")
	sandboxID := parts[0]
	if sandboxID == "" {
		http.Error(w, "sandbox ID is required", http.StatusBadRequest)
		return
	}

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			pm.handleGetSandbox(w, r, sandboxID)
		case http.MethodDelete:
			pm.handleDeleteSandbox(w, r, sandboxID)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	if len(parts) == 2 && parts[1] == "exec" {
		if r.Method == http.MethodPost {
			pm.handleExecSandbox(w, r, sandboxID)
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	http.NotFound(w, r)
}

// handleGetSandbox handles GET /v1/sandboxes/{id}.
func (pm *PoolManager) handleGetSandbox(w http.ResponseWriter, r *http.Request, sandboxID string) {
	rec, err := pm.store.GetSandbox(r.Context(), sandboxID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "sandbox not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("store error: %v", err), http.StatusInternalServerError)
		return
	}

	session := api.SessionState{
		SandboxID:  rec.SandboxID,
		ThreadID:   rec.ThreadID,
		State:      rec.State,
		Host:       rec.HostID,
		Image:      rec.Image,
		LastActive: rec.LastActive,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(session)
}

// handleExecSandbox handles POST /v1/sandboxes/{id}/exec.
func (pm *PoolManager) handleExecSandbox(w http.ResponseWriter, r *http.Request, sandboxID string) {
	ctx := r.Context()
	rec, err := pm.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "sandbox not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("store error: %v", err), http.StatusInternalServerError)
		return
	}

	if rec.State == api.StateReaping || rec.State == api.StateUnreachable {
		http.Error(w, fmt.Sprintf("sandbox is in state %s", rec.State), http.StatusGone)
		return
	}

	var req api.ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid exec payload: %v", err), http.StatusBadRequest)
		return
	}

	// 1. Refresh idle lease
	_ = pm.store.RefreshIdleLease(ctx, sandboxID, pm.cfg.IdleTTL)

	// 2. Mark state Executing
	rec.State = api.StateExecuting
	rec.LastActive = time.Now()
	_ = pm.store.SetSandbox(ctx, rec)

	// 3. Forward exec request to in-pod agent
	execURL := fmt.Sprintf("http://%s/exec", rec.AgentAddr)
	payload, _ := json.Marshal(req)

	client := http.Client{
		Timeout: time.Duration(req.Timeout+10) * time.Second,
	}
	resp, err := client.Post(execURL, "application/json", bytes.NewReader(payload))
	if err != nil {
		rec.State = api.StateUnreachable
		_ = pm.store.SetSandbox(ctx, rec)
		http.Error(w, fmt.Sprintf("agent unreachable: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	var execResp api.ExecResponse
	if err := json.NewDecoder(resp.Body).Decode(&execResp); err != nil {
		http.Error(w, fmt.Sprintf("invalid agent response: %v", err), http.StatusInternalServerError)
		return
	}

	// 4. Return to Bound state and refresh lease
	rec.State = api.StateBound
	rec.LastActive = time.Now()
	_ = pm.store.SetSandbox(ctx, rec)
	_ = pm.store.RefreshIdleLease(ctx, sandboxID, pm.cfg.IdleTTL)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(execResp)
}

// handleDeleteSandbox handles DELETE /v1/sandboxes/{id}.
func (pm *PoolManager) handleDeleteSandbox(w http.ResponseWriter, r *http.Request, sandboxID string) {
	ctx := r.Context()
	rec, err := pm.store.GetSandbox(ctx, sandboxID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		http.Error(w, fmt.Sprintf("store error: %v", err), http.StatusInternalServerError)
		return
	}

	if rec != nil {
		rec.State = api.StateReaping
		_ = pm.store.SetSandbox(ctx, rec)
		_ = pm.provisioner.Destroy(ctx, sandboxID)
		_ = pm.store.DeleteSandbox(ctx, sandboxID)
	}

	w.WriteHeader(http.StatusNoContent)
}

// runReaper executes the background reconciliation loop for idle TTL and deletion.
func (pm *PoolManager) runReaper() {
	ticker := time.NewTicker(pm.cfg.ReaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pm.stopCh:
			return
		case <-ticker.C:
			pm.reapCycle()
		}
	}
}

func (pm *PoolManager) reapCycle() {
	ctx := context.Background()
	sandboxes, err := pm.store.ListSandboxes(ctx, "")
	if err != nil {
		log.Printf("[reaper] failed to list sandboxes: %v", err)
		return
	}

	now := time.Now()
	for _, sb := range sandboxes {
		if sb.State == api.StateBound {
			expired, err := pm.store.IsIdleLeaseExpired(ctx, sb.SandboxID)
			if err == nil && expired {
				sb.State = api.StateIdle
				_ = pm.store.SetSandbox(ctx, sb)
			}
		}

		if sb.State == api.StateIdle {
			if now.Sub(sb.LastActive) > pm.cfg.DeleteAfterStop {
				sb.State = api.StateReaping
				_ = pm.store.SetSandbox(ctx, sb)
				_ = pm.provisioner.Destroy(ctx, sb.SandboxID)
				_ = pm.store.DeleteSandbox(ctx, sb.SandboxID)
			}
		}
	}
}
