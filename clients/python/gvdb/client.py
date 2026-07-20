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


class ImportState:
    """Server-side bulk import job states."""

    PENDING = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3
    CANCELLED = 4


@dataclass
class BackupStatus:
    """Status of a server-side backup job."""

    backup_id: str
    state: str
    shards_total: int
    shards_completed: int
    bytes_uploaded: int
    error_message: str
    elapsed_seconds: float
    manifest_uri: str


@dataclass
class RestoreStatus:
    """Status of a server-side restore job."""

    restore_id: str
    state: str
    shards_total: int
    shards_completed: int
    error_message: str
    elapsed_seconds: float


@dataclass
class BackupInfo:
    """A backup discovered in a backup target."""

    backup_id: str
    collection_name: str
    collection_id: int
    created_at_unix_ms: int
    vector_count: int
    size_bytes: int
    manifest_version: int


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
        resp = self._stub.HealthCheck(
            pb.HealthCheckRequest(), timeout=self._timeout, metadata=self._metadata
        )
        return resp.message

    def get_stats(self) -> dict:
        """Get server statistics."""
        resp = self._stub.GetStats(
            pb.GetStatsRequest(), timeout=self._timeout, metadata=self._metadata
        )
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
            "auto": pb.CreateCollectionRequest.AUTO,
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
            pb.ListCollectionsRequest(), timeout=self._timeout, metadata=self._metadata
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
                v.sparse_vector.CopyFrom(
                    pb.SparseVector(
                        indices=sorted_indices,
                        values=[sv[k] for k in sorted_indices],
                    )
                )
            if ttl_seconds and i < len(ttl_seconds) and ttl_seconds[i] > 0:
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
                yield pb.InsertRequest(
                    collection_name=collection, vectors=proto_vectors
                )

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
            req.filter = filter_expression
        if return_metadata:
            req.return_metadata = True

        resp = self._stub.Search(req, timeout=self._timeout, metadata=self._metadata)
        return [
            SearchResult(
                id=r.id,
                distance=r.distance,
                metadata=_from_proto_metadata(r.metadata)
                if r.metadata.fields
                else None,
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
            req.sparse_query.CopyFrom(
                pb.SparseVector(
                    indices=sorted_indices,
                    values=[sparse_query[k] for k in sorted_indices],
                )
            )
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
                metadata=_from_proto_metadata(r.metadata)
                if r.metadata.fields
                else None,
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

        resp = self._stub.RangeSearch(
            req, timeout=self._timeout, metadata=self._metadata
        )
        return [
            SearchResult(
                id=r.id,
                distance=r.distance,
                metadata=_from_proto_metadata(r.metadata)
                if r.metadata.fields
                else None,
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

    # -- Bulk Import ----------------------------------------------------------

    def import_parquet(self, path, collection, **kwargs):
        """Import vectors from a Parquet file. Requires ``pip install gvdb[parquet]``."""
        from gvdb.importers import import_parquet

        return import_parquet(self, path, collection, **kwargs)

    def import_numpy(self, vectors, collection, **kwargs):
        """Import vectors from a NumPy array. Requires ``pip install gvdb[numpy]``."""
        from gvdb.importers import import_numpy

        return import_numpy(self, vectors, collection, **kwargs)

    def import_dataframe(self, df, collection, **kwargs):
        """Import vectors from a Pandas or Polars DataFrame."""
        from gvdb.importers import import_dataframe

        return import_dataframe(self, df, collection, **kwargs)

    def import_csv(self, path, collection, **kwargs):
        """Import vectors from a CSV file. Requires ``pip install gvdb[pandas]``."""
        from gvdb.importers import import_csv

        return import_csv(self, path, collection, **kwargs)

    def import_h5ad(self, path, collection, **kwargs):
        """Import vectors from an AnnData h5ad file. Requires ``pip install gvdb[h5ad]``."""
        from gvdb.importers import import_h5ad

        return import_h5ad(self, path, collection, **kwargs)

    # -- Server-Side Bulk Import ----------------------------------------------

    def bulk_import(
        self,
        collection: str,
        source_uri: str,
        *,
        format: str = "parquet",
        vector_column: str = "vector",
        id_column: str = "id",
    ) -> str:
        """Start a server-side bulk import from S3/MinIO.

        The server downloads the file from ``source_uri`` and creates
        segments directly, bypassing gRPC overhead for 3-5x throughput
        improvement over streaming insert.

        Args:
            collection: Target collection name (must exist).
            source_uri: S3 URI, e.g. ``s3://bucket/path/file.parquet``.
            format: ``"parquet"`` or ``"numpy"``.
            vector_column: Parquet column name for vectors (default ``"vector"``).
            id_column: Parquet column name for IDs (default ``"id"``).

        Returns:
            Import job ID (string). Poll with :meth:`get_import_status`.
        """
        fmt = pb.PARQUET if format == "parquet" else pb.NUMPY
        resp = self._stub.BulkImport(
            pb.BulkImportRequest(
                collection_name=collection,
                source_uri=source_uri,
                format=fmt,
                vector_column=vector_column,
                id_column=id_column,
            ),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return resp.import_id

    def get_import_status(self, import_id: str) -> dict:
        """Poll the status of a server-side bulk import job.

        Returns:
            Dict with keys: ``import_id``, ``state`` (int, 0=PENDING
            1=RUNNING 2=COMPLETED 3=FAILED 4=CANCELLED),
            ``total_vectors``, ``imported_vectors``, ``progress_percent``,
            ``error_message``, ``elapsed_seconds``, ``segments_created``.
        """
        resp = self._stub.GetImportStatus(
            pb.GetImportStatusRequest(import_id=import_id),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return {
            "import_id": resp.import_id,
            "state": resp.state,
            "total_vectors": resp.total_vectors,
            "imported_vectors": resp.imported_vectors,
            "progress_percent": resp.progress_percent,
            "error_message": resp.error_message,
            "elapsed_seconds": resp.elapsed_seconds,
            "segments_created": resp.segments_created,
        }

    def cancel_import(self, import_id: str) -> bool:
        """Cancel a running or pending import job.

        Returns:
            ``True`` if cancellation was accepted.
        """
        resp = self._stub.CancelImport(
            pb.CancelImportRequest(import_id=import_id),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return resp.success

    def wait_for_import(
        self,
        import_id: str,
        *,
        poll_interval: float = 2.0,
        timeout: float = 3600.0,
    ) -> dict:
        """Block until a bulk import job reaches a terminal state.

        Args:
            import_id: Job ID from :meth:`bulk_import`.
            poll_interval: Seconds between status polls.
            timeout: Maximum seconds to wait before raising.

        Returns:
            Final status dict (same format as :meth:`get_import_status`).

        Raises:
            TimeoutError: If the job does not complete within *timeout*.
        """
        import time

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.get_import_status(import_id)
            if status["state"] in (
                ImportState.COMPLETED,
                ImportState.FAILED,
                ImportState.CANCELLED,
            ):
                return status
            time.sleep(poll_interval)
        raise TimeoutError(f"Import {import_id} did not complete within {timeout}s")

    # -- Backup and Restore -----------------------------------------------------

    def backup_collection(
        self,
        collection: str,
        *,
        s3_bucket: Optional[str] = None,
        s3_prefix: Optional[str] = None,
        local_path: Optional[str] = None,
        backup_id: Optional[str] = None,
    ) -> str:
        """Start a server-side backup of a collection.

        Exactly one of ``s3_bucket`` or ``local_path`` must be provided.

        Args:
            collection: Collection name to back up.
            s3_bucket: S3 bucket the data-node uploads to. Credentials come
                from the data-node's configured object-store credentials.
            s3_prefix: Optional key prefix under the bucket (S3 targets only).
            local_path: Absolute path on the data-node. Must be under the
                server-side allow-list.
            backup_id: Optional client-supplied ID for idempotency. Empty
                lets the server allocate one.

        Returns:
            Backup job ID (string). Poll with :meth:`get_backup_status`.

        Raises:
            ValueError: If not exactly one of ``s3_bucket``/``local_path``
                is provided.
        """
        resp = self._stub.BackupCollection(
            pb.BackupCollectionRequest(
                collection_name=collection,
                target=_to_backup_target(s3_bucket, s3_prefix, local_path),
                backup_id=backup_id or "",
            ),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return resp.backup_id

    def restore_collection(
        self,
        backup_id: str,
        *,
        s3_bucket: Optional[str] = None,
        s3_prefix: Optional[str] = None,
        local_path: Optional[str] = None,
        target_collection: Optional[str] = None,
        overwrite: bool = False,
    ) -> str:
        """Start a server-side restore of a backed-up collection.

        Exactly one of ``s3_bucket`` or ``local_path`` must be provided.

        Args:
            backup_id: ID of the backup to restore.
            s3_bucket: S3 bucket the backup was written to.
            s3_prefix: Optional key prefix under the bucket (S3 sources only).
            local_path: Absolute path on the data-node holding the backup.
            target_collection: Collection to restore into. ``None`` uses the
                collection name from the backup manifest.
            overwrite: If ``True`` and the target collection exists, drop it
                and recreate from the backup. If ``False``, the call fails
                when the target exists.

        Returns:
            Restore job ID (string). Poll with :meth:`get_restore_status`.

        Raises:
            ValueError: If not exactly one of ``s3_bucket``/``local_path``
                is provided.
        """
        resp = self._stub.RestoreCollection(
            pb.RestoreCollectionRequest(
                source=_to_backup_target(s3_bucket, s3_prefix, local_path),
                backup_id=backup_id,
                target_collection_name=target_collection or "",
                overwrite=overwrite,
            ),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return resp.restore_id

    def get_backup_status(self, backup_id: str) -> BackupStatus:
        """Poll the status of a server-side backup job.

        Returns:
            :class:`BackupStatus` with ``state`` as the enum name
            (``"BACKUP_PENDING"``, ``"BACKUP_RUNNING"``, ``"BACKUP_COMPLETED"``,
            ``"BACKUP_FAILED"``, or ``"BACKUP_CANCELLED"``).
            ``manifest_uri`` is empty until the backup completes.
        """
        resp = self._stub.GetBackupStatus(
            pb.GetBackupStatusRequest(backup_id=backup_id),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return BackupStatus(
            backup_id=resp.backup_id,
            state=pb.BackupState.Name(resp.state),
            shards_total=resp.shards_total,
            shards_completed=resp.shards_completed,
            bytes_uploaded=resp.bytes_uploaded,
            error_message=resp.error_message,
            elapsed_seconds=resp.elapsed_seconds,
            manifest_uri=resp.manifest_uri,
        )

    def get_restore_status(self, restore_id: str) -> RestoreStatus:
        """Poll the status of a server-side restore job.

        Returns:
            :class:`RestoreStatus` with ``state`` as the enum name (restore
            reuses the backup states; ``"BACKUP_CANCELLED"`` is unused).
        """
        resp = self._stub.GetRestoreStatus(
            pb.GetRestoreStatusRequest(restore_id=restore_id),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return RestoreStatus(
            restore_id=resp.restore_id,
            state=pb.BackupState.Name(resp.state),
            shards_total=resp.shards_total,
            shards_completed=resp.shards_completed,
            error_message=resp.error_message,
            elapsed_seconds=resp.elapsed_seconds,
        )

    def wait_for_backup(
        self,
        backup_id: str,
        *,
        poll_interval: float = 2.0,
        timeout: float = 3600.0,
    ) -> BackupStatus:
        """Block until a backup job reaches a terminal state.

        Args:
            backup_id: Job ID from :meth:`backup_collection`.
            poll_interval: Seconds between status polls.
            timeout: Maximum seconds to wait before raising.

        Returns:
            Final :class:`BackupStatus` (state ``"BACKUP_COMPLETED"``).

        Raises:
            RuntimeError: If the backup fails or is cancelled.
            TimeoutError: If the job does not complete within *timeout*.
        """
        return self._wait_for_terminal_state(
            "Backup",
            backup_id,
            self.get_backup_status,
            poll_interval=poll_interval,
            timeout=timeout,
        )

    def wait_for_restore(
        self,
        restore_id: str,
        *,
        poll_interval: float = 2.0,
        timeout: float = 3600.0,
    ) -> RestoreStatus:
        """Block until a restore job reaches a terminal state.

        Args:
            restore_id: Job ID from :meth:`restore_collection`.
            poll_interval: Seconds between status polls.
            timeout: Maximum seconds to wait before raising.

        Returns:
            Final :class:`RestoreStatus` (state ``"BACKUP_COMPLETED"``).

        Raises:
            RuntimeError: If the restore fails.
            TimeoutError: If the job does not complete within *timeout*.
        """
        return self._wait_for_terminal_state(
            "Restore",
            restore_id,
            self.get_restore_status,
            poll_interval=poll_interval,
            timeout=timeout,
        )

    def _wait_for_terminal_state(
        self,
        job_kind: str,
        job_id: str,
        get_status,
        *,
        poll_interval: float,
        timeout: float,
    ):
        """Poll a backup or restore job until it reaches a terminal state."""
        import time

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = get_status(job_id)
            if status.state == "BACKUP_COMPLETED":
                return status
            if status.state in ("BACKUP_FAILED", "BACKUP_CANCELLED"):
                raise RuntimeError(
                    f"{job_kind} {job_id} ended in {status.state}: "
                    f"{status.error_message}"
                )
            time.sleep(poll_interval)
        raise TimeoutError(f"{job_kind} {job_id} did not complete within {timeout}s")

    def list_backups(
        self,
        *,
        s3_bucket: Optional[str] = None,
        s3_prefix: Optional[str] = None,
        local_path: Optional[str] = None,
    ) -> list[BackupInfo]:
        """List backups stored in a backup target.

        Exactly one of ``s3_bucket`` or ``local_path`` must be provided.

        Returns:
            List of :class:`BackupInfo`.

        Raises:
            ValueError: If not exactly one of ``s3_bucket``/``local_path``
                is provided.
        """
        resp = self._stub.ListBackups(
            pb.ListBackupsRequest(
                source=_to_backup_target(s3_bucket, s3_prefix, local_path)
            ),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return [
            BackupInfo(
                backup_id=b.backup_id,
                collection_name=b.collection_name,
                collection_id=b.collection_id,
                created_at_unix_ms=b.created_at_unix_ms,
                vector_count=b.vector_count,
                size_bytes=b.size_bytes,
                manifest_version=b.manifest_version,
            )
            for b in resp.backups
        ]

    def cancel_backup(self, backup_id: str) -> bool:
        """Cancel a running or pending backup job.

        Returns:
            ``True`` if cancellation was accepted.
        """
        resp = self._stub.CancelBackup(
            pb.CancelBackupRequest(backup_id=backup_id),
            timeout=self._timeout,
            metadata=self._metadata,
        )
        return resp.success

    # -- Metadata -------------------------------------------------------------

    def update_metadata(
        self,
        collection: str,
        vector_id: int,
        metadata: dict,
        *,
        merge: bool = True,
    ) -> None:
        """Update metadata for a vector."""
        self._stub.UpdateMetadata(
            pb.UpdateMetadataRequest(
                collection_name=collection,
                id=vector_id,
                metadata=_to_proto_metadata(metadata),
                merge=merge,
            ),
            timeout=self._timeout,
            metadata=self._metadata,
        )


def _to_backup_target(
    s3_bucket: Optional[str],
    s3_prefix: Optional[str],
    local_path: Optional[str],
) -> pb.BackupTarget:
    """Build a proto BackupTarget from exactly one of s3_bucket or local_path."""
    if (s3_bucket is None) == (local_path is None):
        raise ValueError("Provide exactly one of s3_bucket or local_path")
    if s3_prefix is not None and s3_bucket is None:
        raise ValueError("s3_prefix requires s3_bucket")
    if s3_bucket is not None:
        return pb.BackupTarget(s3=pb.S3Target(bucket=s3_bucket, prefix=s3_prefix or ""))
    return pb.BackupTarget(local=pb.LocalTarget(path=local_path))


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
