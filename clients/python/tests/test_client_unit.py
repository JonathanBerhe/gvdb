"""Unit tests for GVDBClient that run without a server."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import grpc
import pytest

from gvdb import GVDBClient
from gvdb.client import _to_proto_metadata
from gvdb.pb import vectordb_pb2 as pb


class _ScalarStub:
    """Zero-dimensional scalar stub that implements the NumPy scalar interface."""

    ndim = 0

    def __init__(self, value):
        self._value = value

    def item(self):
        return self._value


class _ArrayStub:
    """One-dimensional array stub that the client must not unwrap into a scalar."""

    ndim = 1

    def item(self):
        return 1


class TestClientChannelOptions:
    def test_default_uses_insecure_channel(self):
        with (
            patch("gvdb.client.grpc.insecure_channel") as insecure_mock,
            patch("gvdb.client.grpc.secure_channel") as secure_mock,
        ):
            client = GVDBClient("localhost:50051")
            insecure_mock.assert_called_once()
            secure_mock.assert_not_called()
            client.close()
            insecure_mock.return_value.close.assert_called_once()

    def test_tls_true_uses_secure_channel_with_default_creds(self):
        with (
            patch("gvdb.client.grpc.ssl_channel_credentials") as ssl_creds,
            patch("gvdb.client.grpc.secure_channel") as secure_mock,
            patch("gvdb.client.grpc.insecure_channel") as insecure_mock,
        ):
            client = GVDBClient("gvdb.example.com:50051", api_key="secret", tls=True)
            ssl_creds.assert_called_once_with()
            secure_mock.assert_called_once()
            assert secure_mock.call_args.args == (
                "gvdb.example.com:50051",
                ssl_creds.return_value,
            )
            insecure_mock.assert_not_called()
            client.close()
            secure_mock.return_value.close.assert_called_once()

    def test_custom_credentials_uses_secure_channel(self):
        creds = MagicMock(spec=grpc.ChannelCredentials)
        with (
            patch("gvdb.client.grpc.secure_channel") as secure_mock,
            patch("gvdb.client.grpc.insecure_channel") as insecure_mock,
        ):
            client = GVDBClient("gvdb.example.com:50051", credentials=creds)
            secure_mock.assert_called_once()
            assert secure_mock.call_args.args == ("gvdb.example.com:50051", creds)
            insecure_mock.assert_not_called()
            client.close()
            secure_mock.return_value.close.assert_called_once()

    def test_custom_channel_not_closed_by_client(self):
        custom_channel = MagicMock(spec=grpc.Channel)
        with (
            patch("gvdb.client.grpc.secure_channel") as secure_mock,
            patch("gvdb.client.grpc.insecure_channel") as insecure_mock,
        ):
            with GVDBClient(channel=custom_channel) as client:
                assert client._channel is custom_channel
            secure_mock.assert_not_called()
            insecure_mock.assert_not_called()
            custom_channel.close.assert_not_called()

    def test_channel_with_tls_or_credentials_raises(self):
        custom_channel = MagicMock(spec=grpc.Channel)
        creds = MagicMock(spec=grpc.ChannelCredentials)
        with pytest.raises(ValueError, match="cannot set 'channel'"):
            GVDBClient(channel=custom_channel, tls=True)
        with pytest.raises(ValueError, match="cannot set 'channel'"):
            GVDBClient(channel=custom_channel, credentials=creds)


class TestGetReturnMetadata:
    def test_get_requests_metadata_by_default(self):
        with GVDBClient("localhost:1") as client:
            client._stub = MagicMock()
            client._stub.Get.return_value = pb.GetResponse(
                vectors=[
                    pb.VectorWithId(
                        id=1,
                        vector=pb.Vector(values=[0.1, 0.2], dimension=2),
                        metadata=pb.Metadata(
                            fields={"tag": pb.MetadataValue(string_value="a")}
                        ),
                    )
                ]
            )
            rows = client.get("docs", [1])
            req = client._stub.Get.call_args.args[0]
            assert req.collection_name == "docs"
            assert list(req.ids) == [1]
            assert req.return_metadata is True
            assert rows == [
                {"id": 1, "vector": pytest.approx([0.1, 0.2]), "metadata": {"tag": "a"}}
            ]

    def test_get_can_disable_return_metadata(self):
        with GVDBClient("localhost:1") as client:
            client._stub = MagicMock()
            client._stub.Get.return_value = pb.GetResponse()
            client.get("docs", [1], return_metadata=False)
            req = client._stub.Get.call_args.args[0]
            assert req.return_metadata is False


class TestBulkImportFormat:
    @pytest.mark.parametrize(
        ("fmt_str", "expected"),
        [
            ("parquet", pb.PARQUET),
            ("PARQUET", pb.PARQUET),
            ("Parquet", pb.PARQUET),
            ("numpy", pb.NUMPY),
            ("NUMPY", pb.NUMPY),
        ],
    )
    def test_valid_formats_case_insensitive(self, fmt_str, expected):
        with GVDBClient("localhost:1") as client:
            client._stub = MagicMock()
            client._stub.BulkImport.return_value = pb.BulkImportResponse(
                import_id="imp-1"
            )
            assert (
                client.bulk_import("docs", "s3://bucket/data", format=fmt_str)
                == "imp-1"
            )
            req = client._stub.BulkImport.call_args.args[0]
            assert req.format == expected

    @pytest.mark.parametrize("bad_fmt", ["csv", "json", "npy", ""])
    def test_invalid_format_raises_value_error(self, bad_fmt):
        with GVDBClient("localhost:1") as client:
            client._stub = MagicMock()
            with pytest.raises(ValueError, match="is not supported"):
                client.bulk_import("docs", "s3://bucket/data", format=bad_fmt)
            client._stub.BulkImport.assert_not_called()


class TestToProtoMetadata:
    def test_unwraps_zero_dim_scalars_and_ignores_arrays(self):
        meta = _to_proto_metadata(
            {
                "flag": _ScalarStub(True),
                "count": _ScalarStub(42),
                "score": _ScalarStub(1.5),
                "name": _ScalarStub("ok"),
                "arr": _ArrayStub(),
            }
        )
        assert meta.fields["flag"].WhichOneof("value") == "bool_value"
        assert meta.fields["flag"].bool_value is True
        assert meta.fields["count"].WhichOneof("value") == "int_value"
        assert meta.fields["count"].int_value == 42
        assert meta.fields["score"].WhichOneof("value") == "double_value"
        assert meta.fields["score"].double_value == 1.5
        assert meta.fields["name"].WhichOneof("value") == "string_value"
        assert meta.fields["name"].string_value == "ok"
        assert "arr" not in meta.fields

    def test_unwraps_numpy_scalars(self):
        np = pytest.importorskip("numpy")
        meta = _to_proto_metadata(
            {
                "i64": np.int64(99),
                "i32": np.int32(-5),
                "u64": np.uint64(7),
                "b": np.bool_(True),
                "f32": np.float32(2.5),
                "arr": np.array([1]),
            }
        )
        assert meta.fields["i64"].int_value == 99
        assert meta.fields["i32"].int_value == -5
        assert meta.fields["u64"].int_value == 7
        assert meta.fields["b"].bool_value is True
        assert meta.fields["f32"].double_value == pytest.approx(2.5)
        assert "arr" not in meta.fields
