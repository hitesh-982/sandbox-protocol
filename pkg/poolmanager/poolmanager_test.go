package poolmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/samsung/agentic-sandbox/pkg/api"
	"github.com/samsung/agentic-sandbox/pkg/store"
)

func setupTestServer(t *testing.T, warmCount int) (*PoolManager, *store.MemoryStore, *LocalProvisioner) {
	st := store.NewMemoryStore()
	prov, err := NewLocalProvisioner("test-node-1", warmCount)
	if err != nil {
		t.Fatalf("failed to create local provisioner: %v", err)
	}

	cfg := Config{
		ListenAddr:      "127.0.0.1:0",
		IdleTTL:         50 * time.Millisecond,
		DeleteAfterStop: 100 * time.Millisecond,
		ReaperInterval:  20 * time.Millisecond,
	}

	pm := New(cfg, st, prov)
	if err := pm.Start(); err != nil {
		t.Fatalf("failed to start pool manager: %v", err)
	}

	return pm, st, prov
}

func TestPoolManager_AcquireWarmAndReconnect(t *testing.T) {
	pm, _, _ := setupTestServer(t, 2)
	defer pm.Stop(context.Background())

	baseURL := "http://" + pm.Addr()

	// 1. First acquire with thread-1 -> warm allocation
	acqReq1 := api.AcquireRequest{
		ThreadID: "thread-1",
		Image:    "test-img",
	}
	body1, _ := json.Marshal(acqReq1)
	resp1, err := http.Post(baseURL+"/v1/sandboxes", "application/json", bytes.NewReader(body1))
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	defer resp1.Body.Close()

	var acqResp1 api.AcquireResponse
	_ = json.NewDecoder(resp1.Body).Decode(&acqResp1)

	if acqResp1.Path != api.PathWarm {
		t.Fatalf("expected path 'warm', got '%s'", acqResp1.Path)
	}
	if acqResp1.SandboxID == "" {
		t.Fatalf("expected non-empty sandbox_id")
	}

	// 2. Second acquire with same thread-1 -> reconnect
	resp2, err := http.Post(baseURL+"/v1/sandboxes", "application/json", bytes.NewReader(body1))
	if err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}
	defer resp2.Body.Close()

	var acqResp2 api.AcquireResponse
	_ = json.NewDecoder(resp2.Body).Decode(&acqResp2)

	if acqResp2.Path != api.PathReconnect {
		t.Fatalf("expected path 'reconnect', got '%s'", acqResp2.Path)
	}
	if acqResp2.SandboxID != acqResp1.SandboxID {
		t.Fatalf("expected same sandbox_id %s, got %s", acqResp1.SandboxID, acqResp2.SandboxID)
	}
}

func TestPoolManager_ColdBoot(t *testing.T) {
	// 0 warm pods available
	pm, _, _ := setupTestServer(t, 0)
	defer pm.Stop(context.Background())

	baseURL := "http://" + pm.Addr()

	acqReq := api.AcquireRequest{
		ThreadID: "thread-cold-1",
		Image:    "test-img",
	}
	body, _ := json.Marshal(acqReq)
	resp, err := http.Post(baseURL+"/v1/sandboxes", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("cold acquire failed: %v", err)
	}
	defer resp.Body.Close()

	var acqResp api.AcquireResponse
	_ = json.NewDecoder(resp.Body).Decode(&acqResp)

	if acqResp.Path != api.PathCold {
		t.Fatalf("expected path 'cold', got '%s'", acqResp.Path)
	}
}

func TestPoolManager_ExecAndSessionState(t *testing.T) {
	pm, _, _ := setupTestServer(t, 1)
	defer pm.Stop(context.Background())

	baseURL := "http://" + pm.Addr()

	// Acquire sandbox
	acqReq := api.AcquireRequest{
		ThreadID: "thread-exec-1",
		Image:    "test-img",
	}
	body, _ := json.Marshal(acqReq)
	resp, _ := http.Post(baseURL+"/v1/sandboxes", "application/json", bytes.NewReader(body))
	var acqResp api.AcquireResponse
	_ = json.NewDecoder(resp.Body).Decode(&acqResp)
	resp.Body.Close()

	// GET session state
	getResp, err := http.Get(baseURL + "/v1/sandboxes/" + acqResp.SandboxID)
	if err != nil {
		t.Fatalf("get sandbox failed: %v", err)
	}
	defer getResp.Body.Close()

	var session api.SessionState
	_ = json.NewDecoder(getResp.Body).Decode(&session)
	if session.State != api.StateBound {
		t.Fatalf("expected state Bound, got %s", session.State)
	}

	// Exec command
	execReq := api.ExecRequest{
		Command: "echo 'hello from sandbox exec'",
		Timeout: 5,
	}
	execBody, _ := json.Marshal(execReq)
	execRespHTTP, err := http.Post(baseURL+"/v1/sandboxes/"+acqResp.SandboxID+"/exec", "application/json", bytes.NewReader(execBody))
	if err != nil {
		t.Fatalf("exec HTTP request failed: %v", err)
	}
	defer execRespHTTP.Body.Close()

	var execResp api.ExecResponse
	_ = json.NewDecoder(execRespHTTP.Body).Decode(&execResp)
	if execResp.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d (stderr: %s)", execResp.ExitCode, execResp.Stderr)
	}
	if execResp.Stdout != "hello from sandbox exec\n" {
		t.Fatalf("unexpected stdout: %q", execResp.Stdout)
	}

	// DELETE sandbox
	delReq, _ := http.NewRequest(http.MethodDelete, baseURL+"/v1/sandboxes/"+acqResp.SandboxID, nil)
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil || delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete failed: %v, status: %v", err, delResp.StatusCode)
	}
}

func TestPoolManager_ReaperIdleTransition(t *testing.T) {
	pm, st, _ := setupTestServer(t, 1)
	defer pm.Stop(context.Background())

	baseURL := "http://" + pm.Addr()

	acqReq := api.AcquireRequest{
		ThreadID: "thread-idle-1",
	}
	body, _ := json.Marshal(acqReq)
	resp, _ := http.Post(baseURL+"/v1/sandboxes", "application/json", bytes.NewReader(body))
	var acqResp api.AcquireResponse
	_ = json.NewDecoder(resp.Body).Decode(&acqResp)
	resp.Body.Close()

	// Wait for idleTTL (50ms) + reaperInterval (20ms) to trigger transition to Idle
	time.Sleep(100 * time.Millisecond)

	rec, err := st.GetSandbox(context.Background(), acqResp.SandboxID)
	if err != nil {
		t.Fatalf("failed to get sandbox record: %v", err)
	}
	if rec.State != api.StateIdle {
		t.Fatalf("expected state to transition to Idle, got %s", rec.State)
	}
}
