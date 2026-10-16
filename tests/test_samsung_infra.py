import os
import shutil
import socket
import subprocess
import time
import pytest

from agent.integrations.samsung_infra import (
    PoolManagerClient,
    SamsungSandbox,
    create_samsung_sandbox,
    SANDBOX_FACTORIES,
    SandboxCommandError,
)


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def pool_manager_server():
    port = find_free_port()
    addr = f"127.0.0.1:{port}"

    # Build the binary first
    build_proc = subprocess.run(
        ["go", "build", "-o", "bin/pool-manager", "./cmd/pool-manager"],
        capture_output=True,
        text=True,
    )
    assert build_proc.returncode == 0, f"go build failed: {build_proc.stderr}"

    proc = subprocess.Popen(
        ["./bin/pool-manager", f"-addr={addr}", "-node-id=test-host", "-warm-count=2"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Wait for server to become ready
    endpoint = f"http://{addr}"
    ready = False
    for _ in range(50):
        try:
            client = PoolManagerClient(endpoint=endpoint)
            # test healthz
            import urllib.request
            with urllib.request.urlopen(f"{endpoint}/healthz", timeout=1) as resp:
                if resp.status == 200:
                    ready = True
                    break
        except Exception:
            time.sleep(0.1)

    assert ready, "Pool Manager server failed to start within 5s"

    yield endpoint

    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()


def test_sandbox_factories_registration():
    assert "samsung" in SANDBOX_FACTORIES
    factory = SANDBOX_FACTORIES["samsung"]
    assert callable(factory)


def test_acquire_and_reconnect(pool_manager_server):
    pm = PoolManagerClient(endpoint=pool_manager_server)

    # 1. First acquire: warm allocation
    thread_id = "thread-open-swe-1"
    sb1 = create_samsung_sandbox(thread_id, pool_manager=pm)
    assert sb1.id.startswith("sb-test-host-")

    # 2. Second acquire with same thread: idempotent reconnect
    sb2 = create_samsung_sandbox(thread_id, pool_manager=pm)
    assert sb2.id == sb1.id


def test_command_execution(pool_manager_server):
    pm = PoolManagerClient(endpoint=pool_manager_server)
    sb = create_samsung_sandbox("thread-exec-test", pool_manager=pm)

    res = sb.execute("echo 'testing 1 2 3'")
    assert res.exit_code == 0
    assert res.stdout.strip() == "testing 1 2 3"
    assert res.stderr == ""

    # Failure command
    res_fail = sb.execute("ls /path_that_does_not_exist_12345")
    assert res_fail.exit_code != 0
    assert "No such file" in res_fail.stderr or res_fail.exit_code != 0


def test_derived_file_operations(pool_manager_server, tmp_path):
    pm = PoolManagerClient(endpoint=pool_manager_server)
    sb = create_samsung_sandbox("thread-files-test", pool_manager=pm)

    # Make a scratch workspace dir inside sandbox
    test_dir = f"/tmp/sb_test_{int(time.time())}"
    file_path = f"{test_dir}/nested/greeting.txt"

    # Write file
    content = "Hello Samsung Infrastructure!\nSecond Line Here.\n"
    sb.write(file_path, content)

    # Read file
    read_back = sb.read(file_path)
    assert read_back == content

    # Edit file
    sb.edit(file_path, "Second Line Here.", "Second Line Replaced.")
    read_edited = sb.read(file_path)
    assert "Second Line Replaced." in read_edited
    assert "Second Line Here." not in read_edited

    # Edit non-existent pattern raises ValueError
    with pytest.raises(ValueError):
        sb.edit(file_path, "NonExistentPattern", "replacement")

    # List dir
    listing = sb.ls(f"{test_dir}/nested")
    assert "greeting.txt" in listing

    # Grep
    grep_res = sb.grep("Replaced", f"{test_dir}/nested")
    assert any("greeting.txt" in line and "Second Line Replaced" in line for line in grep_res)

    # Read non-existent file raises SandboxCommandError
    with pytest.raises(SandboxCommandError):
        sb.read(f"{test_dir}/does_not_exist.txt")

    # Clean up test dir
    sb.execute(f"rm -rf {test_dir}")


def test_delete_sandbox(pool_manager_server):
    pm = PoolManagerClient(endpoint=pool_manager_server)
    sb = create_samsung_sandbox("thread-delete-test", pool_manager=pm)
    sandbox_id = sb.id

    state = pm.get_state(sandbox_id)
    assert state["state"] == "Bound"

    # Close/delete sandbox
    sb.close()

    # Getting state after delete should fail
    with pytest.raises(Exception):
        pm.get_state(sandbox_id)
