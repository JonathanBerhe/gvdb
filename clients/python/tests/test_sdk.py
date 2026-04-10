"""Integration tests for the GVDB Python SDK.

Exercises every public method on GVDBClient against a real server.
Set GVDB_SERVER_ADDR to run against an external server (e.g. kind proxy).
"""

import random

from gvdb import GVDBClient

COLLECTION = "sdk_test_collection"
DIM = 128
NUM_VECTORS = 100


def _random_vector(dim: int = DIM) -> list[float]:
    return [random.gauss(0, 1) for _ in range(dim)]


class TestSDK:
    def test_01_health_check(self, client):
        msg = client.health_check()
        assert isinstance(msg, str)
        assert len(msg) > 0

    def test_02_get_stats(self, client):
        stats = client.get_stats()
        assert "total_collections" in stats
        assert "total_vectors" in stats

    def test_03_create_collection(self, client):
        try:
            client.drop_collection(COLLECTION)
        except Exception:
            pass
        cid = client.create_collection(
            COLLECTION, dimension=DIM, metric="l2", index_type="flat"
        )
        assert isinstance(cid, int)
        assert cid > 0

    def test_04_list_collections(self, client):
        colls = client.list_collections()
        names = [c.name for c in colls]
        assert COLLECTION in names

    def test_05_insert(self, client):
        ids = list(range(1, NUM_VECTORS + 1))
        vectors = [_random_vector() for _ in range(NUM_VECTORS)]
        metadata = [
            {"category": f"cat_{i % 5}", "score": float(i)} for i in range(NUM_VECTORS)
        ]
        count = client.insert(COLLECTION, ids, vectors, metadata=metadata)
        assert count == NUM_VECTORS

    def test_06_stream_insert(self, client):
        start_id = NUM_VECTORS + 1
        n = 200
        ids = list(range(start_id, start_id + n))
        vectors = [_random_vector() for _ in range(n)]
        count = client.stream_insert(COLLECTION, ids, vectors, batch_size=50)
        assert count == n

    def test_07_search(self, client):
        results = client.search(COLLECTION, _random_vector(), top_k=10)
        assert len(results) == 10
        for i in range(1, len(results)):
            assert results[i].distance >= results[i - 1].distance

    def test_08_search_with_filter(self, client):
        results = client.search(
            COLLECTION,
            _random_vector(),
            top_k=5,
            filter_expression="category = 'cat_0'",
            return_metadata=True,
        )
        assert len(results) > 0
        for r in results:
            assert r.metadata is not None
            assert r.metadata["category"] == "cat_0"

    def test_09_hybrid_search(self, client):
        results = client.hybrid_search(
            COLLECTION,
            query_vector=_random_vector(),
            top_k=5,
        )
        assert isinstance(results, list)

    def test_10_range_search(self, client):
        results = client.range_search(
            COLLECTION,
            _random_vector(),
            radius=100000.0,
            max_results=20,
        )
        assert len(results) > 0

    def test_11_get(self, client):
        fetched = client.get(COLLECTION, [1, 2, 3])
        assert len(fetched) == 3
        assert all("id" in v and "vector" in v for v in fetched)

    def test_12_upsert(self, client):
        result = client.upsert(
            COLLECTION,
            ids=[1, 99999],
            vectors=[_random_vector(), _random_vector()],
        )
        assert result["upserted_count"] == 2

    def test_13_update_metadata(self, client):
        # update_metadata should succeed without error
        client.update_metadata(COLLECTION, 5, {"tag": "updated", "price": 42.0})
        # Verify via filtered search — the updated metadata is queryable
        results = client.search(
            COLLECTION,
            _random_vector(),
            top_k=1,
            filter_expression="tag = 'updated'",
            return_metadata=True,
        )
        assert len(results) == 1
        assert results[0].id == 5
        assert results[0].metadata["tag"] == "updated"

    def test_14_delete(self, client):
        deleted = client.delete(COLLECTION, [5, 6, 7])
        assert deleted == 3
        fetched = client.get(COLLECTION, [5, 6, 7])
        assert len(fetched) == 0

    def test_15_drop_collection(self, client):
        client.drop_collection(COLLECTION)
        colls = client.list_collections()
        names = [c.name for c in colls]
        assert COLLECTION not in names

    def test_16_context_manager(self, server_address):
        with GVDBClient(server_address) as c:
            msg = c.health_check()
            assert isinstance(msg, str)
