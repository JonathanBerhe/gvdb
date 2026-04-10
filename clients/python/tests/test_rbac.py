"""RBAC integration tests.

Starts a dedicated gvdb-single-node with auth enabled.
Skipped when GVDB_SERVER_ADDR is set (external server has its own auth config).
"""

import os
import shutil
import random

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
storage:
  data_dir: {data_dir}
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
    import tempfile

    data_dir = tempfile.mkdtemp(prefix="gvdb-rbac-pytest-")
    config = RBAC_CONFIG.format(port=port, collection=COLLECTION, data_dir=data_dir)
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


import subprocess  # noqa: E402 (needed in finally block above)


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
    """Create test collection as admin before RBAC tests."""
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
# Authentication: public endpoints skip auth
# ============================================================================


class TestAuth:
    def test_health_check_no_auth(self, no_auth_client):
        msg = no_auth_client.health_check()
        assert isinstance(msg, str)

    def test_get_stats_no_auth(self, no_auth_client):
        stats = no_auth_client.get_stats()
        assert "total_collections" in stats

    def test_list_collections_no_auth_rejected(self, no_auth_client):
        with pytest.raises(grpc.RpcError) as exc:
            no_auth_client.list_collections()
        assert exc.value.code() in (
            grpc.StatusCode.UNAUTHENTICATED,
            grpc.StatusCode.CANCELLED,
        )

    def test_insert_no_auth_rejected(self, no_auth_client):
        with pytest.raises(grpc.RpcError) as exc:
            no_auth_client.insert(COLLECTION, [999], [_vec()])
        assert exc.value.code() in (
            grpc.StatusCode.UNAUTHENTICATED,
            grpc.StatusCode.CANCELLED,
        )


# ============================================================================
# Admin: full access
# ============================================================================


class TestAdmin:
    def test_search(self, admin_client):
        results = admin_client.search(COLLECTION, _vec(), top_k=5)
        assert len(results) > 0

    def test_insert_and_delete(self, admin_client):
        admin_client.insert(COLLECTION, [500], [_vec()])
        deleted = admin_client.delete(COLLECTION, [500])
        assert deleted == 1

    def test_list_collections(self, admin_client):
        colls = admin_client.list_collections()
        assert any(c.name == COLLECTION for c in colls)


# ============================================================================
# Legacy key: treated as admin
# ============================================================================


class TestLegacy:
    def test_list_collections(self, legacy_client):
        colls = legacy_client.list_collections()
        assert isinstance(colls, list)

    def test_search(self, legacy_client):
        results = legacy_client.search(COLLECTION, _vec(), top_k=3)
        assert len(results) > 0


# ============================================================================
# Readonly: search/get OK, writes denied
# ============================================================================


class TestReadonly:
    def test_search(self, ro_client):
        results = ro_client.search(COLLECTION, _vec(), top_k=5)
        assert len(results) > 0

    def test_get(self, ro_client):
        fetched = ro_client.get(COLLECTION, [1])
        assert len(fetched) == 1

    def test_list_collections(self, ro_client):
        colls = ro_client.list_collections()
        assert isinstance(colls, list)

    def test_insert_denied(self, ro_client):
        with pytest.raises(grpc.RpcError) as exc:
            ro_client.insert(COLLECTION, [900], [_vec()])
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

    def test_delete_denied(self, ro_client):
        with pytest.raises(grpc.RpcError) as exc:
            ro_client.delete(COLLECTION, [1])
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

    def test_upsert_denied(self, ro_client):
        with pytest.raises(grpc.RpcError) as exc:
            ro_client.upsert(COLLECTION, [1], [_vec()])
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED


# ============================================================================
# Readwrite: read+write on allowed collections, denied on others
# ============================================================================


class TestReadwrite:
    def test_search_allowed(self, rw_client):
        results = rw_client.search(COLLECTION, _vec(), top_k=3)
        assert len(results) > 0

    def test_insert_allowed(self, rw_client):
        count = rw_client.insert(COLLECTION, [800], [_vec()])
        assert count == 1

    def test_delete_allowed(self, rw_client):
        deleted = rw_client.delete(COLLECTION, [800])
        assert deleted == 1

    def test_create_collection_denied(self, rw_client):
        with pytest.raises(grpc.RpcError) as exc:
            rw_client.create_collection(
                "rw_blocked", dimension=DIM, metric="l2", index_type="flat"
            )
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

    def test_drop_collection_denied(self, rw_client):
        with pytest.raises(grpc.RpcError) as exc:
            rw_client.drop_collection(COLLECTION)
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

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
