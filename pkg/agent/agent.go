package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/samsung/agentic-sandbox/pkg/api"
)

// Config holds configuration for the in-pod agent.
type Config struct {
	ListenAddr string
	DefaultDir string
}

// Agent serves command execution requests inside the sandbox pod.
type Agent struct {
	cfg        Config
	httpServer *http.Server
	listener   net.Listener
}

// NewAgent creates a new in-pod agent.
func NewAgent(cfg Config) *Agent {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:0" // random available port by default
	}
	if cfg.DefaultDir == "" {
		cfg.DefaultDir = "/workspace"
	}
	a := &Agent{cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("/exec", a.handleExec)
	mux.HandleFunc("/healthz", a.handleHealthz)
	a.httpServer = &http.Server{Handler: mux}
	return a
}

// Start starts listening and serving HTTP requests.
func (a *Agent) Start() error {
	ln, err := net.Listen("tcp", a.cfg.ListenAddr)
	if err != nil {
		return err
	}
	a.listener = ln
	go func() {
		_ = a.httpServer.Serve(ln)
	}()
	return nil
}

// Addr returns the actual listening address (useful when dynamic port is used).
func (a *Agent) Addr() string {
	if a.listener == nil {
		return a.cfg.ListenAddr
	}
	return a.listener.Addr().String()
}

// Stop gracefully stops the agent.
func (a *Agent) Stop(ctx context.Context) error {
	if a.httpServer != nil {
		return a.httpServer.Shutdown(ctx)
	}
	return nil
}

func (a *Agent) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (a *Agent) handleExec(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req api.ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	timeoutSec := req.Timeout
	if timeoutSec <= 0 {
		timeoutSec = 30
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", req.Command)
	if req.WorkDir != "" {
		cmd.Dir = req.WorkDir
	} else if _, err := os.Stat(a.cfg.DefaultDir); err == nil {
		cmd.Dir = a.cfg.DefaultDir
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err := cmd.Run()
	exitCode := 0
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			exitCode = 124
			stderrBuf.WriteString(fmt.Sprintf("\nCommand timed out after %d seconds", timeoutSec))
		} else if exitError, ok := err.(*exec.ExitError); ok {
			if status, ok := exitError.Sys().(syscall.WaitStatus); ok {
				exitCode = status.ExitStatus()
			} else {
				exitCode = 1
			}
		} else {
			exitCode = 1
			stderrBuf.WriteString(fmt.Sprintf("\nCommand execution failed: %v", err))
		}
	}

	resp := api.ExecResponse{
		Stdout:   stdoutBuf.String(),
		Stderr:   stderrBuf.String(),
		ExitCode: exitCode,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
