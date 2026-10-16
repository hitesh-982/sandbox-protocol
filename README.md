# Self-Hosted Agentic Sandbox Orchestration

Implementation of Milestone 1 (M1) of the self-hosted Kubernetes + Kata Containers sandbox orchestrator implementing Open SWE's `SandboxBackendProtocol`.

## Components

1. **Pool Manager (`cmd/pool-manager`, `pkg/poolmanager`)**:
   - Stateless HTTP control plane implementing:
     - `POST /v1/sandboxes`: Idempotent acquire on `thread_id` with `reconnect`, `warm`, or `cold` paths.
     - `GET /v1/sandboxes/{id}`: Serialized session state for reconnect.
     - `POST /v1/sandboxes/{id}/exec`: Forwards execution to in-pod agent, refreshes idle TTL lease.
     - `DELETE /v1/sandboxes/{id}`: Destroys instance and cleans up state.
   - Background **Reaper** enforcing `IDLE_TTL` (transitions to `Idle`) and `DELETE_AFTER_STOP` (teardown).

2. **In-Pod Agent Daemon (`cmd/sandbox-agent`, `pkg/agent`)**:
   - Lightweight daemon running inside each sandbox pod/container.
   - Executes commands with timeout, working directory, and captures `stdout`, `stderr`, and `exit_code`.
   - Exposes `/healthz` for liveness probes.

3. **State Store (`pkg/store`)**:
   - Implements atomic compare-and-swap (CAS) semantics on `/threads/{thread_id}` to resolve racing creates.
   - Stores `/sandboxes/{sandbox_id}` lifecycle metadata.
   - Manages `/leases/idle/{sandbox_id}` TTL leases.

4. **Open SWE Adapter (`agent/integrations/samsung_infra.py`)**:
   - Implements `BaseSandbox` and `SandboxBackendProtocol`.
   - Derives `ls()`, `read()`, `write()`, `edit()`, `glob()`, and `grep()` from `execute()`.
   - Registers in `SANDBOX_FACTORIES["samsung"]`.

## Running and Testing

```bash
# Build both binaries and run the entire test suite (Go + Python):
make test

# Run Go unit tests:
make test-go

# Run Python integration tests:
make test-py

# Start the Pool Manager locally:
make run-pm
```
