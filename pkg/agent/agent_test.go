package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/samsung/agentic-sandbox/pkg/api"
)

func TestAgent_ExecAndHealthz(t *testing.T) {
	ag := NewAgent(Config{ListenAddr: "127.0.0.1:0"})
	if err := ag.Start(); err != nil {
		t.Fatalf("failed to start agent: %v", err)
	}
	defer ag.Stop(context.Background())

	// Test Healthz
	resp, err := http.Get("http://" + ag.Addr() + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz failed: %v, status: %v", err, resp.StatusCode)
	}

	// Test Echo Command
	execReq := api.ExecRequest{
		Command: "echo 'hello samsung sandbox'",
		Timeout: 5,
	}
	body, _ := json.Marshal(execReq)
	postResp, err := http.Post("http://"+ag.Addr()+"/exec", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("exec post failed: %v", err)
	}
	defer postResp.Body.Close()

	var execResp api.ExecResponse
	if err := json.NewDecoder(postResp.Body).Decode(&execResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if execResp.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", execResp.ExitCode)
	}
	if execResp.Stdout != "hello samsung sandbox\n" {
		t.Fatalf("unexpected stdout: %q", execResp.Stdout)
	}

	// Test Non-Zero Exit Code and Stderr
	execReq2 := api.ExecRequest{
		Command: "echo 'error message' >&2; exit 42",
		Timeout: 5,
	}
	body2, _ := json.Marshal(execReq2)
	postResp2, err := http.Post("http://"+ag.Addr()+"/exec", "application/json", bytes.NewReader(body2))
	if err != nil {
		t.Fatalf("exec post failed: %v", err)
	}
	defer postResp2.Body.Close()

	var execResp2 api.ExecResponse
	_ = json.NewDecoder(postResp2.Body).Decode(&execResp2)
	if execResp2.ExitCode != 42 {
		t.Fatalf("expected exit code 42, got %d", execResp2.ExitCode)
	}
	if execResp2.Stderr != "error message\n" {
		t.Fatalf("unexpected stderr: %q", execResp2.Stderr)
	}

	// Test Timeout
	execReq3 := api.ExecRequest{
		Command: "sleep 3",
		Timeout: 1,
	}
	body3, _ := json.Marshal(execReq3)
	start := time.Now()
	postResp3, err := http.Post("http://"+ag.Addr()+"/exec", "application/json", bytes.NewReader(body3))
	duration := time.Since(start)
	if err != nil {
		t.Fatalf("exec post failed: %v", err)
	}
	defer postResp3.Body.Close()

	var execResp3 api.ExecResponse
	_ = json.NewDecoder(postResp3.Body).Decode(&execResp3)
	if execResp3.ExitCode != 124 {
		t.Fatalf("expected timeout exit code 124, got %d (duration: %v)", execResp3.ExitCode, duration)
	}
}
