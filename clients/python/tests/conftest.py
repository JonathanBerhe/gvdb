"""Shared fixtures for GVDB Python SDK tests.

If GVDB_SERVER_ADDR is set, tests run against that external server.
Otherwise, a local gvdb-single-node is started on a free port.
"""

import os
import shutil
import socket
import subprocess
import tempfile
import time

import pytest

from gvdb import GVDBClient


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _wait_for_server(address: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            c = GVDBClient(address, timeout=2.0)
            c.health_check()
            c.close()
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"Server at {address} not ready within {timeout}s")


def _find_server_binary() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "..", "..", ".."))
    return os.path.join(project_root, "build", "bin", "gvdb-single-node")


@pytest.fixture(scope="session")
def server_address():
    """Provide a running GVDB server address.

    Uses GVDB_SERVER_ADDR if set, otherwise starts a local server.
    """
    external = os.environ.get("GVDB_SERVER_ADDR")
    if external:
        _wait_for_server(external)
        yield external
        return

    server_bin = _find_server_binary()
    if not os.path.isfile(server_bin):
        pytest.skip(f"Server binary not found: {server_bin}. Run 'make build' first.")

    port = _find_free_port()
    data_dir = tempfile.mkdtemp(prefix="gvdb-pytest-")
    address = f"localhost:{port}"

    proc = subprocess.Popen(
        [server_bin, "--port", str(port), "--data-dir", data_dir, "--node-id", "1"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        _wait_for_server(address)
        yield address
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        shutil.rmtree(data_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def client(server_address):
    """Session-scoped GVDBClient connected to the test server."""
    c = GVDBClient(server_address)
    yield c
    c.close()


def start_server_with_config(
    config_yaml: str, port: int
) -> tuple[subprocess.Popen, str]:
    """Start a gvdb-single-node with a custom YAML config. Returns (process, address)."""
    server_bin = _find_server_binary()
    if not os.path.isfile(server_bin):
        pytest.skip(f"Server binary not found: {server_bin}. Run 'make build' first.")

    data_dir = tempfile.mkdtemp(prefix="gvdb-pytest-rbac-")
    config_file = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="gvdb-pytest-", delete=False
    )
    config_file.write(config_yaml)
    config_file.close()

    address = f"localhost:{port}"
    proc = subprocess.Popen(
        [
            server_bin,
            "--config",
            config_file.name,
            "--port",
            str(port),
            "--data-dir",
            data_dir,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return proc, address, data_dir, config_file.name
