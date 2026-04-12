"""Bulk data import for GVDB.

Each importer reads a specific format, yields batches of (ids, vectors, metadata),
and delegates to _batch_import for collection management, progress, and error handling.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Optional, Union

import grpc

if TYPE_CHECKING:
    from gvdb.client import GVDBClient

logger = logging.getLogger("gvdb.importers")

Chunk = tuple[list[int], list[list[float]], Optional[list[dict]]]


@dataclass
class ImportResult:
    """Result of a bulk import operation."""

    total_count: int
    batch_count: int
    failed_count: int
    elapsed_seconds: float
    collection: str
    dimension: int
    created_collection: bool

    def __repr__(self) -> str:
        return (
            f"ImportResult(total={self.total_count}, batches={self.batch_count}, "
            f"failed={self.failed_count}, elapsed={self.elapsed_seconds:.1f}s, "
            f"collection='{self.collection}', dim={self.dimension})"
        )


# ---------------------------------------------------------------------------
# Dependency guards
# ---------------------------------------------------------------------------


def _is_missing(val) -> bool:
    """Return True for None, float NaN, and pandas NaT/NA."""
    if val is None:
        return True
    try:
        import math

        if isinstance(val, float) and math.isnan(val):
            return True
    except (TypeError, ValueError):
        pass
    try:
        import pandas as pd

        if pd.isna(val):
            return True
    except Exception:
        pass
    return False


def _require(module_name: str, extra: str):
    """Import a module or raise ImportError with install instructions."""
    import importlib

    try:
        return importlib.import_module(module_name)
    except ImportError:
        raise ImportError(
            f"{module_name} is required for this operation. "
            f"Install with: pip install gvdb[{extra}]"
        ) from None


# ---------------------------------------------------------------------------
# Collection helpers
# ---------------------------------------------------------------------------


def _ensure_collection(
    client: GVDBClient,
    collection: str,
    dimension: int,
    metric: str,
    index_type: str,
) -> bool:
    """Create collection if missing. Returns True if created."""
    for c in client.list_collections():
        if c.name == collection:
            if c.dimension != dimension:
                raise ValueError(
                    f"Collection '{collection}' exists with dimension {c.dimension} "
                    f"but data has dimension {dimension}"
                )
            return False
    client.create_collection(
        collection, dimension=dimension, metric=metric, index_type=index_type
    )
    return True


# ---------------------------------------------------------------------------
# Core batch orchestrator
# ---------------------------------------------------------------------------


def _batch_import(
    client: GVDBClient,
    chunks: Iterator[Chunk],
    collection: str,
    *,
    mode: str = "upsert",
    metric: str = "cosine",
    index_type: str = "auto",
    max_retries: int = 3,
    show_progress: bool = True,
    total_vectors: Optional[int] = None,
) -> ImportResult:
    """Shared import orchestrator. Consumes a chunk generator and sends to GVDB."""
    if mode not in ("upsert", "stream_insert"):
        raise ValueError(f"mode must be 'upsert' or 'stream_insert', got '{mode}'")

    try:
        from tqdm.auto import tqdm
    except ImportError:
        tqdm = None

    start_time = time.monotonic()
    total_count = 0
    batch_count = 0
    failed_count = 0
    dimension = 0
    created = False

    pbar = None
    if show_progress and tqdm is not None:
        pbar = tqdm(total=total_vectors, unit="vec", desc=f"Importing {collection}")

    try:
        for ids, vectors, metadata in chunks:
            if not ids:
                continue

            # Infer dimension and ensure collection on first batch
            if batch_count == 0:
                dimension = len(vectors[0])
                created = _ensure_collection(
                    client, collection, dimension, metric, index_type
                )

            # stream_insert is not idempotent — retrying a partial write
            # causes duplicates, so disable retry for that mode.
            retries = max_retries if mode == "upsert" else 0
            count = _send_batch_with_retry(
                client,
                collection,
                ids,
                vectors,
                metadata,
                mode=mode,
                max_retries=retries,
            )

            if count < 0:
                failed_count += len(ids)
            else:
                total_count += count

            batch_count += 1
            if pbar is not None:
                pbar.update(len(ids))
    finally:
        if pbar is not None:
            pbar.close()

    elapsed = time.monotonic() - start_time
    return ImportResult(
        total_count=total_count,
        batch_count=batch_count,
        failed_count=failed_count,
        elapsed_seconds=round(elapsed, 2),
        collection=collection,
        dimension=dimension,
        created_collection=created,
    )


def _send_batch_with_retry(
    client: GVDBClient,
    collection: str,
    ids: list[int],
    vectors: list[list[float]],
    metadata: Optional[list[dict]],
    *,
    mode: str,
    max_retries: int,
) -> int:
    """Send one batch with retry. Returns count on success, -1 on permanent failure."""
    for attempt in range(max_retries + 1):
        try:
            if mode == "upsert":
                result = client.upsert(collection, ids, vectors, metadata=metadata)
                return result["upserted_count"]
            else:
                return client.stream_insert(collection, ids, vectors, metadata=metadata)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                # Retrying with the same payload will hit the same limit.
                logger.error(
                    "Batch too large (RESOURCE_EXHAUSTED). "
                    "Reduce batch_size and retry. Details: %s",
                    e.details(),
                )
                return -1
            if attempt < max_retries:
                delay = 0.5 * (2**attempt)
                logger.warning(
                    "Batch failed (attempt %d/%d): %s. Retrying in %.1fs...",
                    attempt + 1,
                    max_retries + 1,
                    e.details(),
                    delay,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "Batch permanently failed after %d attempts: %s",
                    max_retries + 1,
                    e.details(),
                )
                return -1
    return -1


# ---------------------------------------------------------------------------
# Format: NumPy
# ---------------------------------------------------------------------------


def import_numpy(
    client: GVDBClient,
    vectors,
    collection: str,
    *,
    ids: Optional[list[int]] = None,
    metadata: Optional[list[dict]] = None,
    batch_size: int = 10_000,
    mode: str = "upsert",
    metric: str = "cosine",
    index_type: str = "auto",
    max_retries: int = 3,
    show_progress: bool = True,
) -> ImportResult:
    """Import vectors from a NumPy array.

    Args:
        vectors: 2D numpy array of shape (N, D).
        collection: Target collection name.
        ids: Optional vector IDs. If None, uses sequential IDs starting from 0.
        metadata: Optional per-vector metadata dicts.
        batch_size: Vectors per batch.
        mode: "upsert" (default, idempotent) or "stream_insert" (faster).
        metric: Distance metric for auto-created collections.
        index_type: Index type for auto-created collections.
    """
    np = _require("numpy", "numpy")

    if not isinstance(vectors, np.ndarray):
        raise TypeError(
            f"Expected numpy.ndarray, got {type(vectors).__name__}. "
            "Use import_parquet() for file paths."
        )
    if vectors.ndim != 2:
        raise ValueError(f"Expected 2D array (N, D), got {vectors.ndim}D")

    n = vectors.shape[0]
    if ids is None:
        ids = list(range(n))

    def _chunks():
        for start in range(0, n, batch_size):
            end = min(start + batch_size, n)
            batch_ids = ids[start:end]
            batch_vecs = vectors[start:end].tolist()
            batch_meta = metadata[start:end] if metadata else None
            yield batch_ids, batch_vecs, batch_meta

    return _batch_import(
        client,
        _chunks(),
        collection,
        mode=mode,
        metric=metric,
        index_type=index_type,
        max_retries=max_retries,
        show_progress=show_progress,
        total_vectors=n,
    )


# ---------------------------------------------------------------------------
# Format: Parquet
# ---------------------------------------------------------------------------


def import_parquet(
    client: GVDBClient,
    path: Union[str, Path],
    collection: str,
    *,
    vector_column: str = "vector",
    id_column: str = "id",
    batch_size: int = 10_000,
    mode: str = "upsert",
    metric: str = "cosine",
    index_type: str = "auto",
    max_retries: int = 3,
    show_progress: bool = True,
) -> ImportResult:
    """Import vectors from a Parquet file.

    Expects the GVDB Parquet schema convention:
    - id column (int64) + vector column (list<float32>) + remaining columns as metadata.

    Args:
        path: Path to Parquet file.
        collection: Target collection name.
        vector_column: Column containing vector data (list of floats).
        id_column: Column containing vector IDs (int).
    """
    pq = _require("pyarrow.parquet", "parquet")

    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")

    pf = pq.ParquetFile(str(path))
    total_rows = pf.metadata.num_rows
    schema = pf.schema_arrow
    all_columns = [f.name for f in schema]

    if vector_column not in all_columns:
        raise ValueError(
            f"Vector column '{vector_column}' not found. "
            f"Available columns: {all_columns}"
        )
    if id_column not in all_columns:
        raise ValueError(
            f"ID column '{id_column}' not found. Available columns: {all_columns}"
        )

    meta_columns = [c for c in all_columns if c not in (vector_column, id_column)]

    def _chunks():
        for batch in pf.iter_batches(batch_size=batch_size):
            batch_ids = batch.column(id_column).to_pylist()
            batch_vecs = batch.column(vector_column).to_pylist()

            batch_meta = None
            if meta_columns:
                batch_meta = []
                meta_arrays = {
                    col: batch.column(col).to_pylist() for col in meta_columns
                }
                for i in range(len(batch_ids)):
                    row = {}
                    for col in meta_columns:
                        val = meta_arrays[col][i]
                        if val is not None:
                            row[col] = val
                    batch_meta.append(row if row else None)

            yield batch_ids, batch_vecs, batch_meta

    return _batch_import(
        client,
        _chunks(),
        collection,
        mode=mode,
        metric=metric,
        index_type=index_type,
        max_retries=max_retries,
        show_progress=show_progress,
        total_vectors=total_rows,
    )


# ---------------------------------------------------------------------------
# Format: DataFrame (Pandas or Polars)
# ---------------------------------------------------------------------------


def _extract_vectors_from_column(series, vector_column: str) -> list[list[float]]:
    """Extract vector lists from a DataFrame column (handles lists, arrays, JSON)."""
    values = series.to_list() if hasattr(series, "to_list") else list(series)
    result = []
    for v in values:
        if isinstance(v, str):
            result.append(json.loads(v))
        elif isinstance(v, (list, tuple)):
            result.append(list(v))
        elif hasattr(v, "tolist"):
            result.append(v.tolist())
        else:
            raise TypeError(
                f"Cannot convert vector value of type {type(v).__name__} "
                f"in column '{vector_column}'"
            )
    return result


def import_dataframe(
    client: GVDBClient,
    df,
    collection: str,
    *,
    vector_column: str = "vector",
    id_column: str = "id",
    batch_size: int = 10_000,
    mode: str = "upsert",
    metric: str = "cosine",
    index_type: str = "auto",
    max_retries: int = 3,
    show_progress: bool = True,
) -> ImportResult:
    """Import vectors from a Pandas or Polars DataFrame.

    Args:
        df: Pandas or Polars DataFrame.
        collection: Target collection name.
        vector_column: Column containing vector data (list/array of floats).
        id_column: Column containing vector IDs (int).
    """
    module = type(df).__module__
    is_pandas = module.startswith("pandas")
    is_polars = module.startswith("polars")

    if not (is_pandas or is_polars):
        raise TypeError(
            f"Expected pandas or polars DataFrame, got {type(df).__name__}. "
            "Use import_numpy() for arrays, import_parquet() for files."
        )

    columns = list(df.columns)
    if vector_column not in columns:
        raise ValueError(
            f"Vector column '{vector_column}' not found. Available columns: {columns}"
        )
    if id_column not in columns:
        raise ValueError(
            f"ID column '{id_column}' not found. Available columns: {columns}"
        )

    meta_columns = [c for c in columns if c not in (vector_column, id_column)]
    n = len(df)

    def _slice_df(start, end):
        if is_pandas:
            return df.iloc[start:end]
        return df.slice(start, end - start)

    def _chunks():
        for start in range(0, n, batch_size):
            end = min(start + batch_size, n)
            chunk = _slice_df(start, end)

            if is_pandas:
                batch_ids = chunk[id_column].tolist()
            else:
                batch_ids = chunk[id_column].to_list()

            batch_vecs = _extract_vectors_from_column(
                chunk[vector_column], vector_column
            )

            batch_meta = None
            if meta_columns:
                batch_meta = []
                for i in range(len(batch_ids)):
                    row = {}
                    for col in meta_columns:
                        if is_pandas:
                            val = chunk[col].iloc[i]
                        else:
                            val = chunk[col][i]
                        if not _is_missing(val):
                            row[col] = val
                    batch_meta.append(row if row else None)

            yield batch_ids, batch_vecs, batch_meta

    return _batch_import(
        client,
        _chunks(),
        collection,
        mode=mode,
        metric=metric,
        index_type=index_type,
        max_retries=max_retries,
        show_progress=show_progress,
        total_vectors=n,
    )


# ---------------------------------------------------------------------------
# Format: CSV
# ---------------------------------------------------------------------------


def import_csv(
    client: GVDBClient,
    path: Union[str, Path],
    collection: str,
    *,
    vector_column: str = "vector",
    id_column: str = "id",
    batch_size: int = 10_000,
    mode: str = "upsert",
    metric: str = "cosine",
    index_type: str = "auto",
    max_retries: int = 3,
    show_progress: bool = True,
) -> ImportResult:
    """Import vectors from a CSV file.

    Supports two vector encodings (auto-detected):
    1. JSON-encoded list column: ``"[0.1, 0.2, 0.3]"``
    2. Dimension-prefixed columns: ``vector_0, vector_1, ..., vector_N``

    Args:
        path: Path to CSV file.
        collection: Target collection name.
        vector_column: Name of the vector column (or prefix for dimension columns).
        id_column: Column containing vector IDs.
    """
    pd = _require("pandas", "pandas")

    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")

    # Read first row to detect vector format
    header = pd.read_csv(str(path), nrows=0)
    columns = list(header.columns)

    if vector_column in columns:
        # JSON-encoded vector column
        vec_mode = "json"
    else:
        # Look for dimension-prefixed columns
        prefixed = sorted(
            [
                c
                for c in columns
                if c.startswith(f"{vector_column}_") and c.rsplit("_", 1)[1].isdigit()
            ],
            key=lambda c: int(c.rsplit("_", 1)[1]),
        )
        if not prefixed:
            raise ValueError(
                f"Vector column '{vector_column}' not found, and no "
                f"dimension-prefixed columns (e.g., {vector_column}_0, "
                f"{vector_column}_1, ...) found. Available columns: {columns}"
            )
        vec_mode = "prefixed"

    def _chunks():
        reader = pd.read_csv(str(path), chunksize=batch_size)
        for chunk in reader:
            batch_ids = chunk[id_column].tolist()

            if vec_mode == "json":
                batch_vecs = [json.loads(v) for v in chunk[vector_column]]
            else:
                batch_vecs = chunk[prefixed].values.tolist()

            # Metadata: all columns except id and vector columns
            if vec_mode == "json":
                skip = {vector_column, id_column}
            else:
                skip = set(prefixed) | {id_column}
            meta_cols = [c for c in chunk.columns if c not in skip]

            batch_meta = None
            if meta_cols:
                batch_meta = []
                for _, row in chunk[meta_cols].iterrows():
                    d = {k: v for k, v in row.items() if not _is_missing(v)}
                    batch_meta.append(d if d else None)

            yield batch_ids, batch_vecs, batch_meta

    return _batch_import(
        client,
        _chunks(),
        collection,
        mode=mode,
        metric=metric,
        index_type=index_type,
        max_retries=max_retries,
        show_progress=show_progress,
        total_vectors=None,  # CSV has no row count in header
    )


# ---------------------------------------------------------------------------
# Format: h5ad (AnnData)
# ---------------------------------------------------------------------------


def import_h5ad(
    client: GVDBClient,
    path: Union[str, Path],
    collection: str,
    *,
    embedding_key: str = "X_pca",
    id_column: Optional[str] = None,
    metadata_columns: Optional[list[str]] = None,
    batch_size: int = 10_000,
    mode: str = "upsert",
    metric: str = "cosine",
    index_type: str = "auto",
    max_retries: int = 3,
    show_progress: bool = True,
) -> ImportResult:
    """Import vectors from an AnnData h5ad file.

    Args:
        path: Path to .h5ad file.
        collection: Target collection name.
        embedding_key: Key in adata.obsm for embeddings (default "X_pca").
        id_column: Column in adata.obs for IDs. None uses sequential IDs.
        metadata_columns: Columns from adata.obs to include. None uses all.
    """
    ad = _require("anndata", "h5ad")

    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")

    adata = ad.read_h5ad(str(path))

    if embedding_key not in adata.obsm:
        raise KeyError(
            f"Embedding key '{embedding_key}' not found in adata.obsm. "
            f"Available keys: {list(adata.obsm.keys())}"
        )

    embeddings = adata.obsm[embedding_key]
    n = embeddings.shape[0]

    # Handle sparse matrices
    try:
        import scipy.sparse

        is_sparse = scipy.sparse.issparse(embeddings)
    except ImportError:
        is_sparse = False

    # IDs
    if id_column is not None:
        if id_column not in adata.obs.columns:
            raise ValueError(
                f"ID column '{id_column}' not found in adata.obs. "
                f"Available: {list(adata.obs.columns)}"
            )
        all_ids = adata.obs[id_column].tolist()
    else:
        all_ids = list(range(n))

    # Metadata columns
    if metadata_columns is None:
        meta_cols = list(adata.obs.columns)
    else:
        missing = [c for c in metadata_columns if c not in adata.obs.columns]
        if missing:
            raise ValueError(
                f"Metadata columns not found in adata.obs: {missing}. "
                f"Available: {list(adata.obs.columns)}"
            )
        meta_cols = list(metadata_columns)

    # Filter to serializable dtypes only
    obs = adata.obs
    serializable_cols = []
    for col in meta_cols:
        dtype = obs[col].dtype
        if dtype.kind in ("i", "u", "f", "b", "U", "O"):
            serializable_cols.append(col)
    meta_cols = serializable_cols

    def _chunks():
        for start in range(0, n, batch_size):
            end = min(start + batch_size, n)
            batch_ids = all_ids[start:end]

            if is_sparse:
                batch_vecs = embeddings[start:end].toarray().tolist()
            else:
                batch_vecs = embeddings[start:end].tolist()

            batch_meta = None
            if meta_cols:
                batch_meta = []
                obs_slice = obs.iloc[start:end]
                for _, row in obs_slice[meta_cols].iterrows():
                    d = {}
                    for col in meta_cols:
                        val = row[col]
                        if not _is_missing(val):
                            d[col] = val
                    batch_meta.append(d if d else None)

            yield batch_ids, batch_vecs, batch_meta

    return _batch_import(
        client,
        _chunks(),
        collection,
        mode=mode,
        metric=metric,
        index_type=index_type,
        max_retries=max_retries,
        show_progress=show_progress,
        total_vectors=n,
    )
