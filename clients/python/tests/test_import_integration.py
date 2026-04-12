"""Integration tests for bulk importers — requires running GVDB server."""

from __future__ import annotations

import json

import pytest

np = pytest.importorskip("numpy")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pd = pytest.importorskip("pandas")

DIM = 32
IMPORT_PREFIX = "import_int_test"


def _cleanup(client, name):
    try:
        client.drop_collection(name)
    except Exception:
        pass


class TestImportIntegration:
    def test_import_numpy(self, client):
        coll = f"{IMPORT_PREFIX}_numpy"
        _cleanup(client, coll)

        vectors = np.random.rand(100, DIM).astype(np.float32)
        result = client.import_numpy(
            vectors, coll, ids=list(range(100)), show_progress=False
        )
        assert result.total_count == 100
        assert result.dimension == DIM
        assert result.created_collection is True

        # Verify data is searchable
        results = client.search(coll, vectors[0].tolist(), top_k=1)
        assert len(results) == 1
        assert results[0].id == 0

        _cleanup(client, coll)

    def test_import_parquet(self, client, tmp_path):
        coll = f"{IMPORT_PREFIX}_parquet"
        _cleanup(client, coll)

        n = 50
        ids = list(range(n))
        vectors = [[float(i + j) / n for j in range(DIM)] for i in range(n)]
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

        result = client.import_parquet(str(path), coll, show_progress=False)
        assert result.total_count == n
        assert result.dimension == DIM

        # Verify via get
        fetched = client.get(coll, [0, 1, 2])
        assert len(fetched) == 3

        _cleanup(client, coll)

    def test_import_csv_json_vectors(self, client, tmp_path):
        coll = f"{IMPORT_PREFIX}_csv"
        _cleanup(client, coll)

        n = 30
        path = tmp_path / "test.csv"
        df = pd.DataFrame(
            {
                "id": list(range(n)),
                "vector": [
                    json.dumps([float(i + j) / n for j in range(DIM)]) for i in range(n)
                ],
                "label": [f"lab_{i % 5}" for i in range(n)],
            }
        )
        df.to_csv(str(path), index=False)

        result = client.import_csv(str(path), coll, show_progress=False)
        assert result.total_count == n

        _cleanup(client, coll)

    def test_import_dataframe_pandas(self, client):
        coll = f"{IMPORT_PREFIX}_df"
        _cleanup(client, coll)

        n = 40
        df = pd.DataFrame(
            {
                "id": list(range(n)),
                "vector": [np.random.rand(DIM).tolist() for _ in range(n)],
                "score": [float(i) for i in range(n)],
            }
        )

        result = client.import_dataframe(df, coll, show_progress=False)
        assert result.total_count == n

        _cleanup(client, coll)

    def test_resume_idempotent(self, client):
        """Import the same data twice with upsert — count should stay the same."""
        coll = f"{IMPORT_PREFIX}_resume"
        _cleanup(client, coll)

        vectors = np.random.rand(20, DIM).astype(np.float32)
        ids = list(range(20))

        r1 = client.import_numpy(vectors, coll, ids=ids, show_progress=False)
        assert r1.total_count == 20

        # Import again — upsert is idempotent
        r2 = client.import_numpy(vectors, coll, ids=ids, show_progress=False)
        assert r2.total_count == 20
        assert r2.created_collection is False  # Already exists

        # Total vectors should still be 20, not 40
        stats_after = client.get(coll, ids)
        assert len(stats_after) == 20

        _cleanup(client, coll)

    def test_auto_creates_collection(self, client):
        coll = f"{IMPORT_PREFIX}_autocreate"
        _cleanup(client, coll)

        vectors = np.random.rand(5, DIM).astype(np.float32)
        result = client.import_numpy(vectors, coll, show_progress=False)
        assert result.created_collection is True

        colls = client.list_collections()
        names = [c.name for c in colls]
        assert coll in names

        _cleanup(client, coll)

    def test_dimension_mismatch_raises(self, client):
        coll = f"{IMPORT_PREFIX}_dim_mismatch"
        _cleanup(client, coll)

        # Create collection with DIM=32
        client.create_collection(coll, dimension=DIM, metric="l2", index_type="flat")

        # Try to import DIM=64 vectors
        vectors = np.random.rand(5, 64).astype(np.float32)
        with pytest.raises(ValueError, match="dimension"):
            client.import_numpy(vectors, coll, show_progress=False)

        _cleanup(client, coll)
