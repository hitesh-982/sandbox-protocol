"""
Samsung Infrastructure Adapter for Open SWE SandboxBackendProtocol.

Implements BaseSandbox protocol backed by Samsung's self-hosted
Kubernetes + Kata Containers Pool Manager.
"""

from __future__ import annotations

import base64
import json
import os
import shlex
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


@dataclass
class ExecuteResponse:
    """Outcome of a command execution."""
    stdout: str
    stderr: str
    exit_code: int

    @property
    def output(self) -> str:
        """Combined output for easy inspection."""
        return self.stdout + self.stderr


@dataclass
class AcquireResult:
    """Metadata returned from sandbox acquisition."""
    sandbox_id: str
    host_id: str
    path: str  # reconnect, warm, cold


class SandboxError(Exception):
    """Base exception for sandbox operations."""
    pass


class SandboxCommandError(SandboxError):
    """Raised when a derived file or utility command fails unexpectedly."""
    def __init__(self, message: str, response: ExecuteResponse):
        super().__init__(f"{message}: exit {response.exit_code}\nstderr: {response.stderr}")
        self.response = response


class BaseSandbox:
    """
    Open SWE BaseSandbox interface.
    Subclasses only need to implement execute(); ls, read, write, edit, glob, grep
    are derived from execute().
    """

    @property
    def id(self) -> str:
        raise NotImplementedError

    def execute(self, command: str, timeout: Optional[int] = None) -> ExecuteResponse:
        raise NotImplementedError

    def read(self, path: str) -> str:
        """Read text content of a file."""
        res = self.execute(f"cat {shlex.quote(path)}")
        if res.exit_code != 0:
            raise SandboxCommandError(f"Failed to read file '{path}'", res)
        return res.stdout

    def write(self, path: str, content: str) -> None:
        """Write text content to a file, creating parent directories if needed."""
        # Use base64 encoding to avoid escaping and newline issues in shell
        b64_content = base64.b64encode(content.encode("utf-8")).decode("ascii")
        parent_dir = os.path.dirname(path)
        mkdir_cmd = f"mkdir -p {shlex.quote(parent_dir)} && " if parent_dir else ""
        cmd = f"{mkdir_cmd}printf '%s' {shlex.quote(b64_content)} | base64 -d > {shlex.quote(path)}"
        res = self.execute(cmd)
        if res.exit_code != 0:
            raise SandboxCommandError(f"Failed to write file '{path}'", res)

    def edit(self, path: str, old_str: str, new_str: str) -> None:
        """Replace occurrences of old_str with new_str in a file."""
        content = self.read(path)
        if old_str not in content:
            raise ValueError(f"Pattern '{old_str}' not found in file '{path}'")
        new_content = content.replace(old_str, new_str)
        self.write(path, new_content)

    def ls(self, path: str = ".") -> List[str]:
        """List files and directories in path."""
        res = self.execute(f"ls -1 {shlex.quote(path)}")
        if res.exit_code != 0:
            raise SandboxCommandError(f"Failed to list directory '{path}'", res)
        lines = [line.strip() for line in res.stdout.splitlines() if line.strip()]
        return lines

    def glob(self, pattern: str, path: str = ".") -> List[str]:
        """Find files matching glob pattern."""
        script = (
            f"import glob, os; "
            f"matches = glob.glob(os.path.join({repr(path)}, {repr(pattern)}), recursive=True); "
            f"print('\\n'.join(matches))"
        )
        res = self.execute(f"python3 -c {shlex.quote(script)}")
        if res.exit_code != 0:
            # Fallback to find if python3 is unavailable
            res = self.execute(f"find {shlex.quote(path)} -name {shlex.quote(pattern)}")
            if res.exit_code != 0:
                raise SandboxCommandError(f"Failed to glob '{pattern}' in '{path}'", res)
        lines = [line.strip() for line in res.stdout.splitlines() if line.strip()]
        return sorted(lines)

    def grep(self, pattern: str, path: str = ".") -> List[str]:
        """Search for text pattern in path."""
        res = self.execute(f"grep -rn {shlex.quote(pattern)} {shlex.quote(path)}")
        # grep exit code 1 means pattern not found, which is normal
        if res.exit_code not in (0, 1):
            raise SandboxCommandError(f"grep error on '{pattern}' in '{path}'", res)
        lines = [line.strip() for line in res.stdout.splitlines() if line.strip()]
        return lines


class PoolManagerClient:
    """Client for the stateless Pool Manager HTTP service."""

    def __init__(self, endpoint: Optional[str] = None):
        self.endpoint = (endpoint or os.environ.get("POOL_MANAGER_ENDPOINT", "http://127.0.0.1:8080")).rstrip("/")

    def acquire(
        self,
        thread_id: str,
        image: Optional[str] = None,
        manifest: Optional[Dict[str, Any]] = None,
        capabilities: Optional[List[str]] = None,
    ) -> AcquireResult:
        """Idempotently acquire a sandbox for thread_id."""
        url = f"{self.endpoint}/v1/sandboxes"
        payload = {
            "thread_id": thread_id,
            "image": image or "samsung/default-kata:latest",
            "manifest": manifest or {},
            "capabilities": capabilities or [],
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                body = json.loads(resp.read().decode("utf-8"))
                return AcquireResult(
                    sandbox_id=body["sandbox_id"],
                    host_id=body["host_id"],
                    path=body["path"],
                )
        except urllib.error.URLError as e:
            raise SandboxError(f"Failed to acquire sandbox: {e}") from e

    def exec(self, sandbox_id: str, command: str, timeout: Optional[int] = None) -> ExecuteResponse:
        """Run a command inside sandbox_id."""
        url = f"{self.endpoint}/v1/sandboxes/{sandbox_id}/exec"
        payload = {
            "command": command,
            "timeout": timeout or 30,
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                body = json.loads(resp.read().decode("utf-8"))
                return ExecuteResponse(
                    stdout=body.get("stdout", ""),
                    stderr=body.get("stderr", ""),
                    exit_code=body.get("exit_code", 0),
                )
        except urllib.error.URLError as e:
            raise SandboxError(f"Failed to execute command in sandbox {sandbox_id}: {e}") from e

    def get_state(self, sandbox_id: str) -> Dict[str, Any]:
        """Fetch serialized session state."""
        url = f"{self.endpoint}/v1/sandboxes/{sandbox_id}"
        req = urllib.request.Request(url)
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError as e:
            raise SandboxError(f"Failed to get sandbox state: {e}") from e

    def delete(self, sandbox_id: str) -> None:
        """Tear down sandbox now."""
        url = f"{self.endpoint}/v1/sandboxes/{sandbox_id}"
        req = urllib.request.Request(url, method="DELETE")
        try:
            with urllib.request.urlopen(req) as resp:
                pass
        except urllib.error.URLError as e:
            raise SandboxError(f"Failed to delete sandbox {sandbox_id}: {e}") from e


class SamsungSandbox(BaseSandbox):
    """
    Open SWE SamsungSandbox implementation wrapping the Pool Manager.
    """

    def __init__(self, sandbox_id: str, pool_manager: PoolManagerClient):
        self._id = sandbox_id
        self._pm = pool_manager

    @property
    def id(self) -> str:
        return self._id

    def execute(self, command: str, timeout: Optional[int] = None) -> ExecuteResponse:
        return self._pm.exec(self._id, command, timeout=timeout)

    def close(self) -> None:
        """Explicitly release the sandbox."""
        self._pm.delete(self._id)


# Global Pool Manager client instance
_default_pm: Optional[PoolManagerClient] = None


def get_default_pool_manager() -> PoolManagerClient:
    global _default_pm
    if _default_pm is None:
        _default_pm = PoolManagerClient()
    return _default_pm


def set_default_pool_manager(pm: PoolManagerClient) -> None:
    global _default_pm
    _default_pm = pm


def create_samsung_sandbox(thread_id: str, pool_manager: Optional[PoolManagerClient] = None) -> SamsungSandbox:
    """
    Factory function registered in SANDBOX_FACTORIES for Open SWE.
    """
    pm = pool_manager or get_default_pool_manager()
    acq = pm.acquire(thread_id)
    return SamsungSandbox(acq.sandbox_id, pm)


# Registry for Open SWE backend providers
SANDBOX_FACTORIES: Dict[str, Callable[[str], BaseSandbox]] = {
    "samsung": create_samsung_sandbox,
}
