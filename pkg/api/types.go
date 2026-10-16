package api

import "time"

// AcquirePath indicates how the sandbox was acquired.
type AcquirePath string

const (
	PathReconnect AcquirePath = "reconnect"
	PathWarm      AcquirePath = "warm"
	PathCold      AcquirePath = "cold"
)

// SandboxState represents the lifecycle state of a sandbox.
type SandboxState string

const (
	StateRequested     SandboxState = "Requested"
	StateAssigning     SandboxState = "Assigning"
	StateBound         SandboxState = "Bound"
	StateExecuting     SandboxState = "Executing"
	StateIdle          SandboxState = "Idle"
	StateReaping       SandboxState = "Reaping"
	StateColdBooting   SandboxState = "ColdBooting"
	StateUnreachable   SandboxState = "Unreachable"
	StateStaleSentinel SandboxState = "StaleSentinel"
)

// AcquireRequest is the payload sent to POST /v1/sandboxes.
type AcquireRequest struct {
	ThreadID     string                 `json:"thread_id"`
	Image        string                 `json:"image,omitempty"`
	Manifest     map[string]interface{} `json:"manifest,omitempty"`
	Capabilities []string               `json:"capabilities,omitempty"`
}

// AcquireResponse is returned from POST /v1/sandboxes.
type AcquireResponse struct {
	SandboxID string      `json:"sandbox_id"`
	HostID    string      `json:"host_id"`
	Path      AcquirePath `json:"path"`
}

// SessionState is returned from GET /v1/sandboxes/{id}.
type SessionState struct {
	SandboxID  string       `json:"sandbox_id"`
	ThreadID   string       `json:"thread_id"`
	State      SandboxState `json:"state"`
	Host       string       `json:"host"`
	Image      string       `json:"image,omitempty"`
	LastActive time.Time    `json:"last_active"`
}

// ExecRequest is the payload sent to POST /v1/sandboxes/{id}/exec.
type ExecRequest struct {
	Command string `json:"command"`
	Timeout int    `json:"timeout,omitempty"` // in seconds, default 30
	WorkDir string `json:"work_dir,omitempty"`
}

// ExecResponse is returned from POST /v1/sandboxes/{id}/exec.
type ExecResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}
