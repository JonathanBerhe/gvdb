"""GVDB Python client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import grpc

from gvdb.pb import vectordb_pb2 as pb
from gvdb.pb import vectordb_pb2_grpc as pb_grpc


@dataclass
class SearchResult:
    """A single search result."""
    id: int
    distance: float
    metadata: Optional[dict] = None


@dataclass
class CollectionInfo:
    """Collection metadata."""
    name: str
    id: int
    dimension: int
    vector_count: int


class GVDBClient:
    """Client for GVDB distributed vector database.

    Example::

        client = GVDBClient("localhost:50051", api_key="your-key")
        client.create_collection("docs", dimension=768)
        client.insert("docs", ids=[1, 2], vectors=[[0.1, ...], [0.2, ...]])
        results = client.search("docs", query_vector=[0.1, ...], top_k=10)
        client.close()
    """

    def __init__(
        self,
        address: str = "localhost:50051",
        *,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self._address = address
        self._timeout = timeout
        self._metadata = ()
        if api_key:
            self._metadata = (("authorization", f"Bearer {api_key}"),)
        self._channel = grpc.insecure_channel(
            address,
            options=[
                ("grpc.max_send_message_length", 256 * 1024 * 1024),
                ("grpc.max_receive_message_length", 256 * 1024 * 1024),
            ],
        )
        self._stub = pb_grpc.VectorDBServiceStub(self._channel)

    def close(self):
        """Close the connection."""
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # -- Health ---------------------------------------------------------------

    def health_check(self) -> str:
        """Check server health. Returns status message."""
        resp = self._stub.HealthCheck(pb.HealthCheckRequest(), timeout=self._timeout, metadata=self._metadata)
        return resp.message

    def get_stats(self) -> dict:
        """Get server statistics."""
        resp = self._stub.GetStats(pb.GetStatsRequest(), timeout=self._timeout, metadata=self._metadata)
        return {
            "total_collections": resp.total_collections,
            "total_vectors": resp.total_vectors,
            "total_queries": resp.total_queries,
            "avg_query_time_ms": resp.avg_query_time_ms,
        }

    # -- Collections ----------------------------------------------------------

    def create_collection(
        self,
        name: str,
        *,
        dimension: int,
        metric: str = "l2",
        index_type: str = "hnsw",
        num_shards: int = 0,
    ) -> int:
        """Create a collection. Returns collection ID."""
        metric_map = {
            "l2": pb.CreateCollectionRequest.L2,
            "ip": pb.CreateCollectionRequest.INNER_PRODUCT,
            "cosine": pb.CreateCollectionRequest.COSINE,
        }
        index_map = {
            "flat": pb.CreateCollectionRequest.FLAT,
            "hnsw": pb.CreateCollectionRequest.HNSW,
            "ivf_flat": pb.CreateCollectionRequest.IVF_FLAT,
            "ivf_pq": pb.CreateCollectionRequest.IVF_PQ,
            "ivf_sq": pb.CreateCollectionRequest.IVF_SQ,
            "turboquant": pb.CreateCollectionRequest.TURBOQUANT,
            "ivf_turboquant": pb.CreateCollectionRequest.IVF_TURBOQUANT,
        }
        resp = self._stub.CreateCollection(
            pb.CreateCollectionRequest(
                collection_name=name,
                dimension=dimension,
                metric=metric_map[metric.lower()],
                index_type=index_map[index_type.lower()],
                num_shards=num_shards,
            ),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return resp.collection_id

    def drop_collection(self, name: str) -> None:
        """Drop a collection."""
        self._stub.DropCollection(
            pb.DropCollectionRequest(collection_name=name),
            timeout=self._timeout,
            metadata=self._metadata,
        )

    def list_collections(self) -> list[CollectionInfo]:
        """List all collections."""
        resp = self._stub.ListCollections(
            pb.ListCollectionsRequest(), timeout=self._timeout
        )
        return [
            CollectionInfo(
                name=c.collection_name,
                id=c.collection_id,
                dimension=c.dimension,
                vector_count=c.vector_count,
            )
            for c in resp.collections
        ]

    # -- Vectors --------------------------------------------------------------

    def insert(
        self,
        collection: str,
        ids: list[int],
        vectors: list[list[float]],
        metadata: Optional[list[dict]] = None,
        sparse_vectors: Optional[list[dict[int, float]]] = None,
        ttl_seconds: Optional[list[int]] = None,
    ) -> int:
        """Insert vectors. Returns number inserted.

        Args:
            sparse_vectors: Optional list of sparse vectors as {dim_index: value} dicts.
            ttl_seconds: Optional per-vector TTL in seconds (0 = no expiration).
        """
        proto_vectors = []
        for i, (vid, vec) in enumerate(zip(ids, vectors)):
            v = pb.VectorWithId(
                id=vid,
                vector=pb.Vector(values=vec, dimension=len(vec)),
            )
            if metadata and i < len(metadata) and metadata[i]:
                v.metadata.CopyFrom(_to_proto_metadata(metadata[i]))
            if sparse_vectors and i < len(sparse_vectors) and sparse_vectors[i]:
                sv = sparse_vectors[i]
                sorted_indices = sorted(sv.keys())
                v.sparse_vector.CopyFrom(pb.SparseVector(
                    indices=sorted_indices,
                    values=[sv[k] for k in sorted_indices],
                ))
            if ttl_seconds and i < len(ttl_seconds) and ttl_seconds[i]:
                v.ttl_seconds = ttl_seconds[i]
            proto_vectors.append(v)

        resp = self._stub.Insert(
            pb.InsertRequest(collection_name=collection, vectors=proto_vectors),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return resp.inserted_count

    def stream_insert(
        self,
        collection: str,
        ids: list[int],
        vectors: list[list[float]],
        *,
        batch_size: int = 10000,
        metadata: Optional[list[dict]] = None,
    ) -> int:
        """Stream insert vectors in batches. Returns total number inserted."""

        def _chunks():
            for start in range(0, len(ids), batch_size):
                end = min(start + batch_size, len(ids))
                proto_vectors = []
                for i in range(start, end):
                    v = pb.VectorWithId(
                        id=ids[i],
                        vector=pb.Vector(values=vectors[i], dimension=len(vectors[i])),
                    )
                    if metadata and i < len(metadata) and metadata[i]:
                        v.metadata.CopyFrom(_to_proto_metadata(metadata[i]))
                    proto_vectors.append(v)
                yield pb.InsertRequest(collection_name=collection, vectors=proto_vectors)

        resp = self._stub.StreamInsert(
            _chunks(),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return resp.inserted_count

    def search(
        self,
        collection: str,
        query_vector: list[float],
        *,
        top_k: int = 10,
        filter_expression: str = "",
        return_metadata: bool = False,
    ) -> list[SearchResult]:
        """Search for similar vectors."""
        req = pb.SearchRequest(
            collection_name=collection,
            query_vector=pb.Vector(values=query_vector, dimension=len(query_vector)),
            top_k=top_k,
        )
        if filter_expression:
            req.filter_expression = filter_expression
        if return_metadata:
            req.return_metadata = True

        resp = self._stub.Search(req, timeout=self._timeout, metadata=self._metadata)
        return [
            SearchResult(
                id=r.id,
                distance=r.distance,
                metadata=_from_proto_metadata(r.metadata) if r.metadata.fields else None,
            )
            for r in resp.results
        ]

    def hybrid_search(
        self,
        collection: str,
        *,
        query_vector: Optional[list[float]] = None,
        text_query: str = "",
        sparse_query: Optional[dict[int, float]] = None,
        top_k: int = 10,
        vector_weight: float = 0.5,
        text_weight: float = 0.5,
        sparse_weight: float = 0.0,
        text_field: str = "text",
        filter_expression: str = "",
        return_metadata: bool = False,
    ) -> list[SearchResult]:
        """Hybrid search combining vector similarity, BM25 text, and sparse retrieval."""
        req = pb.HybridSearchRequest(
            collection_name=collection,
            text_query=text_query,
            top_k=top_k,
            vector_weight=vector_weight,
            text_weight=text_weight,
            text_field=text_field,
            sparse_weight=sparse_weight,
        )
        if query_vector:
            req.query_vector.CopyFrom(
                pb.Vector(values=query_vector, dimension=len(query_vector))
            )
        if sparse_query:
            sorted_indices = sorted(sparse_query.keys())
            req.sparse_query.CopyFrom(pb.SparseVector(
                indices=sorted_indices,
                values=[sparse_query[k] for k in sorted_indices],
            ))
        if filter_expression:
            req.filter = filter_expression
        if return_metadata:
            req.return_metadata = True

        resp = self._stub.HybridSearch(
            req,
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return [
            SearchResult(
                id=r.id,
                distance=r.distance,
                metadata=_from_proto_metadata(r.metadata) if r.metadata.fields else None,
            )
            for r in resp.results
        ]

    def upsert(
        self,
        collection: str,
        ids: list[int],
        vectors: list[list[float]],
        metadata: Optional[list[dict]] = None,
    ) -> dict:
        """Upsert vectors (insert or replace). Returns counts."""
        proto_vectors = []
        for i, (vid, vec) in enumerate(zip(ids, vectors)):
            v = pb.VectorWithId(
                id=vid,
                vector=pb.Vector(values=vec, dimension=len(vec)),
            )
            if metadata and i < len(metadata) and metadata[i]:
                v.metadata.CopyFrom(_to_proto_metadata(metadata[i]))
            proto_vectors.append(v)

        resp = self._stub.Upsert(
            pb.UpsertRequest(collection_name=collection, vectors=proto_vectors),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return {
            "upserted_count": resp.upserted_count,
            "inserted_count": resp.inserted_count,
            "updated_count": resp.updated_count,
        }

    def range_search(
        self,
        collection: str,
        query_vector: list[float],
        *,
        radius: float,
        filter_expression: str = "",
        return_metadata: bool = False,
        max_results: int = 1000,
    ) -> list[SearchResult]:
        """Find all vectors within a distance radius."""
        req = pb.RangeSearchRequest(
            collection_name=collection,
            query_vector=pb.Vector(values=query_vector, dimension=len(query_vector)),
            radius=radius,
            max_results=max_results,
        )
        if filter_expression:
            req.filter = filter_expression
        if return_metadata:
            req.return_metadata = True

        resp = self._stub.RangeSearch(req, timeout=self._timeout, metadata=self._metadata)
        return [
            SearchResult(
                id=r.id,
                distance=r.distance,
                metadata=_from_proto_metadata(r.metadata) if r.metadata.fields else None,
            )
            for r in resp.results
        ]

    def get(self, collection: str, ids: list[int]) -> list[dict]:
        """Get vectors by ID. Returns list of {id, vector, metadata}."""
        resp = self._stub.Get(
            pb.GetRequest(collection_name=collection, ids=ids),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        results = []
        for v in resp.vectors:
            entry = {"id": v.id, "vector": list(v.vector.values)}
            if v.metadata.fields:
                entry["metadata"] = _from_proto_metadata(v.metadata)
            results.append(entry)
        return results

    def delete(self, collection: str, ids: list[int]) -> int:
        """Delete vectors by ID. Returns number deleted."""
        resp = self._stub.Delete(
            pb.DeleteRequest(collection_name=collection, ids=ids),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return resp.deleted_count

    def update_metadata(
        self,
        collection: str,
        vector_id: int,
        metadata: dict,
        *,
        merge: bool = True,
    ) -> None:
        """Update metadata for a vector."""
        mode = (
            pb.UpdateMetadataRequest.MERGE
            if merge
            else pb.UpdateMetadataRequest.REPLACE
        )
        self._stub.UpdateMetadata(
            pb.UpdateMetadataRequest(
                collection_name=collection,
                vector_id=vector_id,
                metadata=_to_proto_metadata(metadata),
                mode=mode,
            ),
            timeout=self._timeout,
            metadata=self._metadata,
        )


def _to_proto_metadata(meta: dict) -> pb.Metadata:
    """Convert a Python dict to proto Metadata."""
    fields = {}
    for k, v in meta.items():
        if isinstance(v, bool):
            fields[k] = pb.MetadataValue(bool_value=v)
        elif isinstance(v, int):
            fields[k] = pb.MetadataValue(int_value=v)
        elif isinstance(v, float):
            fields[k] = pb.MetadataValue(double_value=v)
        elif isinstance(v, str):
            fields[k] = pb.MetadataValue(string_value=v)
    return pb.Metadata(fields=fields)


def _from_proto_metadata(meta: pb.Metadata) -> dict:
    """Convert proto Metadata to Python dict."""
    result = {}
    for k, v in meta.fields.items():
        which = v.WhichOneof("value")
        if which == "int_value":
            result[k] = v.int_value
        elif which == "double_value":
            result[k] = v.double_value
        elif which == "string_value":
            result[k] = v.string_value
        elif which == "bool_value":
            result[k] = v.bool_value
    return result
