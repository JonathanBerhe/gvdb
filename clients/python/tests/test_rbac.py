"""RBAC integration tests for the SDK against a real auth-enabled server.

These tests start a dedicated gvdb-single-node from a YAML config with
server.auth set, and check two things end to end:

- The SDK path: the server accepts the API key that GVDBClient sends, and
  server errors reach the caller as a raw grpc.RpcError with the server
  status code.
- The YAML auth wiring: the legacy api_keys list and each role family
  (admin, readonly with "*", readwrite with a collection list) are parsed
  from the config file and enforced by the real binary.

The full permission matrix (every role against every operation) is covered
by the C++ tests in test/unit/rbac_test.cpp and
test/unit/auth_processor_test.cpp. The check that every GVDBClient method
sends the key is in test_client_metadata.py. Skipped when GVDB_SERVER_ADDR
is set, because an external server has its own auth config.
"""

import os
import random
import shutil
import subprocess

import grpc
import pytest

from gvdb import GVDBClient
from tests.conftest import _find_free_port, _wait_for_server, start_server_with_config

COLLECTION = "rbac_sdk_test"
DIM = 4

RBAC_CONFIG = """
server:
  grpc_port: {port}
  auth:
    enabled: true
    api_keys: ["legacy-key"]
    roles:
      - key: "admin-key"
        role: admin
      - key: "rw-key"
        role: readwrite
        collections: ["{collection}", "second_coll"]
      - key: "ro-key"
        role: readonly
        collections: ["*"]
consensus:
  node_id: 1
  single_node_mode: true
logging:
  level: info
index:
  default_index_type: flat
"""


def _vec(dim: int = DIM) -> list[float]:
    return [random.random() for _ in range(dim)]


@pytest.fixture(scope="module")
def rbac_server():
    if os.environ.get("GVDB_SERVER_ADDR"):
        pytest.skip("RBAC tests need a dedicated auth server")

    port = _find_free_port()
    config = RBAC_CONFIG.format(port=port, collection=COLLECTION)
    proc, address, data_dir, config_path = start_server_with_config(config, port)

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
        os.unlink(config_path)


@pytest.fixture(scope="module")
def admin_client(rbac_server):
    c = GVDBClient(rbac_server, api_key="admin-key")
    yield c
    c.close()


@pytest.fixture(scope="module")
def legacy_client(rbac_server):
    c = GVDBClient(rbac_server, api_key="legacy-key")
    yield c
    c.close()


@pytest.fixture(scope="module")
def rw_client(rbac_server):
    c = GVDBClient(rbac_server, api_key="rw-key")
    yield c
    c.close()


@pytest.fixture(scope="module")
def ro_client(rbac_server):
    c = GVDBClient(rbac_server, api_key="ro-key")
    yield c
    c.close()


@pytest.fixture(scope="module")
def no_auth_client(rbac_server):
    c = GVDBClient(rbac_server)
    yield c
    c.close()


@pytest.fixture(scope="module", autouse=True)
def _setup_collection(admin_client):
    """Create the test collection with ids 1-10 as admin.

    Every test reads only this data or data it writes itself, so the tests
    do not depend on their order. The create and insert calls are not
    wrapped in try/except, so this fixture also checks that the YAML admin
    role can create a collection and insert.
    """
    try:
        admin_client.drop_collection(COLLECTION)
    except Exception:
        pass
    admin_client.create_collection(
        COLLECTION, dimension=DIM, metric="l2", index_type="flat"
    )
    admin_client.insert(COLLECTION, list(range(1, 11)), [_vec() for _ in range(10)])
    yield
    try:
        admin_client.drop_collection(COLLECTION)
    except Exception:
        pass


# ============================================================================
# No key: the SDK sends no authorization metadata and the call is rejected
# ============================================================================


class TestAuth:
    def test_list_collections_no_auth_rejected(self, no_auth_client):
        # The auth interceptor rejects a call by cancelling it, so the client
        # can see CANCELLED instead of UNAUTHENTICATED.
        with pytest.raises(grpc.RpcError) as exc:
            no_auth_client.list_collections()
        assert exc.value.code() in (
            grpc.StatusCode.UNAUTHENTICATED,
            grpc.StatusCode.CANCELLED,
        )


# ============================================================================
# Legacy key from server.auth.api_keys: treated as admin
# ============================================================================


class TestLegacy:
    def test_search(self, legacy_client):
        results = legacy_client.search(COLLECTION, _vec(), top_k=3)
        assert len(results) > 0


# ============================================================================
# Readonly with collections ["*"]: reads allowed, writes denied
# ============================================================================


class TestReadonly:
    def test_get(self, ro_client):
        fetched = ro_client.get(COLLECTION, [1])
        assert len(fetched) == 1

    def test_upsert_denied(self, ro_client):
        with pytest.raises(grpc.RpcError) as exc:
            ro_client.upsert(COLLECTION, [1], [_vec()])
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED


# ============================================================================
# Readwrite with a collection list: writes allowed only on listed collections
# ============================================================================


class TestReadwrite:
    def test_insert_allowed(self, rw_client):
        count = rw_client.insert(COLLECTION, [800], [_vec()])
        assert count == 1

    def test_other_collection_denied(self, rw_client, admin_client):
        admin_client.create_collection(
            "forbidden_coll", dimension=DIM, metric="l2", index_type="flat"
        )
        try:
            with pytest.raises(grpc.RpcError) as exc:
                rw_client.insert("forbidden_coll", [1], [_vec()])
            assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED
        finally:
            admin_client.drop_collection("forbidden_coll")
