"""Unit tests for the authorization metadata on GVDBClient calls. No server required.

Every GVDBClient method that calls the gRPC stub must pass
metadata=(("authorization", "Bearer <key>"),) when the client has an
api_key, and empty metadata when it has none.

The list of methods to check comes from the GVDBClient source: every
self._stub.<RPC> reference in a method is one case. A new method that
calls the stub therefore gets a case automatically, and the case fails
until CALLS below says how to call the method.

The stub is a MagicMock limited to the RPC names of VectorDBService. Each
RPC returns an empty response of its real proto type, so methods that
convert the response run to the end. StreamInsert receives a request
generator that the mock does not read. The metadata is a keyword argument
of the call, so the check is the same as for the unary RPCs.
"""

from __future__ import annotations

import ast
import inspect
from unittest.mock import MagicMock

import pytest
from google.protobuf.message_factory import GetMessageClass

from gvdb import GVDBClient
from gvdb.pb import vectordb_pb2 as pb

SERVICE = pb.DESCRIPTOR.services_by_name["VectorDBService"]
VEC = [0.1, 0.2, 0.3, 0.4]

# How to call each GVDBClient method that calls the stub, with valid arguments.
CALLS = {
    "health_check": lambda c: c.health_check(),
    "get_stats": lambda c: c.get_stats(),
    "create_collection": lambda c: c.create_collection("docs", dimension=4),
    "drop_collection": lambda c: c.drop_collection("docs"),
    "list_collections": lambda c: c.list_collections(),
    "insert": lambda c: c.insert("docs", [1], [VEC]),
    "stream_insert": lambda c: c.stream_insert("docs", [1], [VEC]),
    "search": lambda c: c.search("docs", VEC),
    "hybrid_search": lambda c: c.hybrid_search("docs", query_vector=VEC),
    "upsert": lambda c: c.upsert("docs", [1], [VEC]),
    "range_search": lambda c: c.range_search("docs", VEC, radius=1.0),
    "get": lambda c: c.get("docs", [1]),
    "delete": lambda c: c.delete("docs", [1]),
    "bulk_import": lambda c: c.bulk_import("docs", "s3://bucket/vectors.parquet"),
    "get_import_status": lambda c: c.get_import_status("imp-1"),
    "cancel_import": lambda c: c.cancel_import("imp-1"),
    "backup_collection": lambda c: c.backup_collection("docs", local_path="/bk"),
    "restore_collection": lambda c: c.restore_collection("bk-1", local_path="/bk"),
    "get_backup_status": lambda c: c.get_backup_status("bk-1"),
    "get_restore_status": lambda c: c.get_restore_status("rs-1"),
    "list_backups": lambda c: c.list_backups(local_path="/bk"),
    "cancel_backup": lambda c: c.cancel_backup("bk-1"),
    "update_metadata": lambda c: c.update_metadata("docs", 1, {"tag": "a"}),
}


def _stub_calls() -> list[tuple[str, str]]:
    """Return (method, RPC) for every self._stub.<RPC> reference in GVDBClient."""
    cls = ast.parse(inspect.getsource(GVDBClient)).body[0]
    calls = set()
    for method in cls.body:
        if not isinstance(method, ast.FunctionDef):
            continue
        for node in ast.walk(method):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Attribute)
                and node.value.attr == "_stub"
                and isinstance(node.value.value, ast.Name)
                and node.value.value.id == "self"
            ):
                calls.add((method.name, node.attr))
    return sorted(calls)


STUB_CALLS = _stub_calls()


def _offline_stub() -> MagicMock:
    """A mock stub that has only the service RPCs, each returning an empty response."""
    stub = MagicMock(spec=[m.name for m in SERVICE.methods])
    for m in SERVICE.methods:
        getattr(stub, m.name).return_value = GetMessageClass(m.output_type)()
    return stub


def test_every_stub_call_has_a_case():
    assert set(CALLS) == {method for method, _ in STUB_CALLS}


@pytest.mark.parametrize(
    ("api_key", "expected"),
    [("k", (("authorization", "Bearer k"),)), (None, ())],
    ids=["with_key", "without_key"],
)
@pytest.mark.parametrize(
    ("method", "rpc"), STUB_CALLS, ids=[f"{m}-{r}" for m, r in STUB_CALLS]
)
def test_stub_call_metadata(method, rpc, api_key, expected):
    if method not in CALLS:
        pytest.fail(f"GVDBClient.{method} calls the stub: add it to CALLS")
    with GVDBClient("localhost:1", api_key=api_key) as client:
        client._stub = _offline_stub()
        CALLS[method](client)
        rpc_mock = getattr(client._stub, rpc)
        rpc_mock.assert_called_once()
        assert rpc_mock.call_args.kwargs.get("metadata") == expected
