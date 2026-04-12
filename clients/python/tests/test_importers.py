"""Unit tests for gvdb.importers — no server required."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from gvdb.importers import (
    ImportResult,
    _batch_import,
    _ensure_collection,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_client(collections=None):
    """Return a mock GVDBClient with list_collections/create_collection/upsert."""
    client = MagicMock()
    client.list_collections.return_value = collections or []
    client.create_collection.return_value = 1
    client.upsert.return_value = {
        "upserted_count": 0,
        "inserted_count": 0,
        "updated_count": 0,
    }
    client.stream_insert.return_value = 0
    return client


def _make_collection_info(name, dim):
    info = MagicMock()
    info.name = name
    info.dimension = dim
    return info


# ---------------------------------------------------------------------------
# _ensure_collection
# ---------------------------------------------------------------------------


class TestEnsureCollection:
    def test_creates_when_missing(self):
        client = _mock_client()
        created = _ensure_collection(client, "new_coll", 128, "cosine", "auto")
        assert created is True
        client.create_collection.assert_called_once_with(
            "new_coll", dimension=128, metric="cosine", index_type="auto"
        )

    def test_skips_when_exists_matching_dim(self):
        client = _mock_client([_make_collection_info("existing", 128)])
        created = _ensure_collection(client, "existing", 128, "cosine", "auto")
        assert created is False
        client.create_collection.assert_not_called()

    def test_raises_on_dimension_mismatch(self):
        client = _mock_client([_make_collection_info("existing", 64)])
        with pytest.raises(ValueError, match="dimension 64.*dimension 128"):
            _ensure_collection(client, "existing", 128, "cosine", "auto")


# ---------------------------------------------------------------------------
# _batch_import
# ---------------------------------------------------------------------------


class TestBatchImport:
    def test_upsert_mode_calls_upsert(self):
        client = _mock_client()
        client.upsert.return_value = {
            "upserted_count": 3,
            "inserted_count": 3,
            "updated_count": 0,
        }

        chunks = iter([([1, 2, 3], [[0.1] * 4] * 3, None)])
        result = _batch_import(
            client, chunks, "test", mode="upsert", show_progress=False
        )
        assert result.total_count == 3
        assert result.batch_count == 1
        assert result.failed_count == 0
        assert result.dimension == 4
        client.upsert.assert_called_once()

    def test_stream_insert_mode(self):
        client = _mock_client()
        client.stream_insert.return_value = 5

        chunks = iter([([1, 2, 3, 4, 5], [[0.1] * 4] * 5, None)])
        result = _batch_import(
            client, chunks, "test", mode="stream_insert", show_progress=False
        )
        assert result.total_count == 5
        client.stream_insert.assert_called_once()

    def test_invalid_mode_raises(self):
        client = _mock_client()
        with pytest.raises(ValueError, match="mode must be"):
            _batch_import(client, iter([]), "test", mode="bad", show_progress=False)

    def test_empty_generator(self):
        client = _mock_client()
        result = _batch_import(client, iter([]), "test", show_progress=False)
        assert result.total_count == 0
        assert result.batch_count == 0

    def test_skips_empty_batches(self):
        client = _mock_client()
        client.upsert.return_value = {
            "upserted_count": 2,
            "inserted_count": 2,
            "updated_count": 0,
        }

        chunks = iter([([], [], None), ([1, 2], [[0.1] * 4] * 2, None)])
        result = _batch_import(client, chunks, "test", show_progress=False)
        assert result.total_count == 2
        assert result.batch_count == 1

    def test_auto_creates_collection(self):
        client = _mock_client()
        client.upsert.return_value = {
            "upserted_count": 1,
            "inserted_count": 1,
            "updated_count": 0,
        }

        chunks = iter([([1], [[0.5] * 8], None)])
        result = _batch_import(client, chunks, "new_coll", show_progress=False)
        assert result.created_collection is True
        assert result.dimension == 8
        client.create_collection.assert_called_once()


# ---------------------------------------------------------------------------
# import_numpy
# ---------------------------------------------------------------------------


np = pytest.importorskip("numpy")


class TestImportNumpy:
    def test_basic_array(self):
        from gvdb.importers import import_numpy

        client = _mock_client()
        client.upsert.return_value = {
            "upserted_count": 10,
            "inserted_count": 10,
            "updated_count": 0,
        }
        vectors = np.random.rand(10, 4).astype(np.float32)

        result = import_numpy(
            client, vectors, "test", ids=list(range(10)), show_progress=False
        )
        assert result.total_count == 10
        assert result.dimension == 4

    def test_auto_generated_ids(self):
        from gvdb.importers import import_numpy

        client = _mock_client()
        client.upsert.return_value = {
            "upserted_count": 5,
            "inserted_count": 5,
            "updated_count": 0,
        }
        vectors = np.random.rand(5, 8).astype(np.float32)

        result = import_numpy(client, vectors, "test", show_progress=False)
        assert result.total_count == 5
        # Verify IDs 0-4 were passed
        call_args = client.upsert.call_args
        assert call_args[0][1] == [0, 1, 2, 3, 4]

    def test_with_metadata(self):
        from gvdb.importers import import_numpy

        client = _mock_client()
        client.upsert.return_value = {
            "upserted_count": 3,
            "inserted_count": 3,
            "updated_count": 0,
        }
        vectors = np.random.rand(3, 4).astype(np.float32)
        metadata = [{"label": "a"}, {"label": "b"}, {"label": "c"}]

        import_numpy(client, vectors, "test", metadata=metadata, show_progress=False)
        call_args = client.upsert.call_args
        assert call_args[1]["metadata"] == metadata

    def test_wrong_type_raises(self):
        from gvdb.importers import import_numpy

        client = _mock_client()
        with pytest.raises(TypeError, match="Expected numpy.ndarray.*str"):
            import_numpy(client, "data.parquet", "test", show_progress=False)

    def test_wrong_ndim_raises(self):
        from gvdb.importers import import_numpy

        client = _mock_client()
        with pytest.raises(ValueError, match="Expected 2D"):
            import_numpy(client, np.array([1, 2, 3]), "test", show_progress=False)

    def test_batching(self):
        from gvdb.importers import import_numpy

        client = _mock_client()
        client.upsert.return_value = {
            "upserted_count": 5,
            "inserted_count": 5,
            "updated_count": 0,
        }
        vectors = np.random.rand(12, 4).astype(np.float32)

        result = import_numpy(
            client, vectors, "test", batch_size=5, show_progress=False
        )
        assert result.batch_count == 3  # 5 + 5 + 2
        assert client.upsert.call_count == 3


# ---------------------------------------------------------------------------
# import_parquet
# ---------------------------------------------------------------------------


pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


class TestImportParquet:
    def _write_parquet(self, tmp_path, n=20, dim=4):
        import pyarrow as pa
        import pyarrow.parquet as pq

        ids = list(range(n))
        vectors = [[float(i + j) for j in range(dim)] for i in range(n)]
        categories = [f"cat_{i % 3}" for i in range(n)]

        table = pa.table(
            {
                "id": ids,
                "vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "category": categories,
            }
        )
        path = tmp_path / "test.parquet"
        pq.write_table(table, str(path))
        return path

    def test_basic_parquet(self, tmp_path):
        from gvdb.importers import import_parquet

        path = self._write_parquet(tmp_path, n=10, dim=4)
        client = _mock_client()
        client.upsert.return_value = {
            "upserted_count": 10,
            "inserted_count": 10,
            "updated_count": 0,
        }

        result = import_parquet(client, path, "test", show_progress=False)
        assert result.total_count == 10
        assert result.dimension == 4

    def test_metadata_columns_auto_mapped(self, tmp_path):
        from gvdb.importers import import_parquet

        path = self._write_parquet(tmp_path, n=5, dim=4)
        client = _mock_client()
        client.upsert.return_value = {
            "upserted_count": 5,
            "inserted_count": 5,
            "updated_count": 0,
        }

        import_parquet(client, path, "test", show_progress=False)
        call_args = client.upsert.call_args
        metadata = call_args[1]["metadata"]
        assert metadata is not None
        assert "category" in metadata[0]

    def test_missing_vector_column_raises(self, tmp_path):
        from gvdb.importers import import_parquet

        path = self._write_parquet(tmp_path)
        client = _mock_client()
        with pytest.raises(ValueError, match="Vector column 'embeddings' not found"):
            import_parquet(
                client, path, "test", vector_column="embeddings", show_progress=False
            )

    def test_missing_file_raises(self):
        from gvdb.importers import import_parquet

        client = _mock_client()
        with pytest.raises(FileNotFoundError):
            import_parquet(client, "/nonexistent.parquet", "test", show_progress=False)


# ---------------------------------------------------------------------------
# import_csv
# ---------------------------------------------------------------------------


pd = pytest.importorskip("pandas")


class TestImportCSV:
    def test_json_encoded_vectors(self, tmp_path):
        from gvdb.importers import import_csv

        import pandas

        path = tmp_path / "test.csv"
        df = pandas.DataFrame(
            {
                "id": list(range(10)),
                "vector": [json.dumps([float(i)] * 4) for i in range(10)],
                "label": [f"cat_{i % 3}" for i in range(10)],
            }
        )
        df.to_csv(str(path), index=False)

        client = _mock_client()
        client.upsert.return_value = {
            "upserted_count": 10,
            "inserted_count": 10,
            "updated_count": 0,
        }

        result = import_csv(client, path, "test", show_progress=False)
        assert result.total_count == 10

    def test_prefixed_columns(self, tmp_path):
        from gvdb.importers import import_csv

        header = "id,vector_0,vector_1,vector_2,label"
        rows = [f"{i},{i * 0.1},{i * 0.2},{i * 0.3},cat_{i}" for i in range(5)]
        path = tmp_path / "test.csv"
        path.write_text(header + "\n" + "\n".join(rows))

        client = _mock_client()
        client.upsert.return_value = {
            "upserted_count": 5,
            "inserted_count": 5,
            "updated_count": 0,
        }

        result = import_csv(client, path, "test", show_progress=False)
        assert result.total_count == 5
        assert result.dimension == 3

    def test_missing_vector_column_raises(self, tmp_path):
        from gvdb.importers import import_csv

        path = tmp_path / "test.csv"
        path.write_text("id,data\n1,hello\n")

        client = _mock_client()
        with pytest.raises(ValueError, match="Vector column.*not found"):
            import_csv(client, path, "test", show_progress=False)


# ---------------------------------------------------------------------------
# import_dataframe
# ---------------------------------------------------------------------------


class TestImportDataframe:
    def test_pandas_df(self):
        import pandas

        from gvdb.importers import import_dataframe

        df = pandas.DataFrame(
            {
                "id": [1, 2, 3],
                "vector": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]],
                "label": ["a", "b", "c"],
            }
        )
        client = _mock_client()
        client.upsert.return_value = {
            "upserted_count": 3,
            "inserted_count": 3,
            "updated_count": 0,
        }

        result = import_dataframe(client, df, "test", show_progress=False)
        assert result.total_count == 3
        assert result.dimension == 2

    def test_wrong_type_raises(self):
        from gvdb.importers import import_dataframe

        client = _mock_client()
        with pytest.raises(TypeError, match="Expected pandas or polars"):
            import_dataframe(
                client, {"not": "a dataframe"}, "test", show_progress=False
            )

    def test_missing_column_raises(self):
        import pandas

        from gvdb.importers import import_dataframe

        df = pandas.DataFrame({"id": [1], "data": [0.5]})
        client = _mock_client()
        with pytest.raises(ValueError, match="Vector column.*not found"):
            import_dataframe(client, df, "test", show_progress=False)


# ---------------------------------------------------------------------------
# ImportResult
# ---------------------------------------------------------------------------


class TestImportResult:
    def test_repr(self):
        r = ImportResult(
            total_count=100,
            batch_count=2,
            failed_count=0,
            elapsed_seconds=1.23,
            collection="test",
            dimension=128,
            created_collection=True,
        )
        s = repr(r)
        assert "total=100" in s
        assert "test" in s
