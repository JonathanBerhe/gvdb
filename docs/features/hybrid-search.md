# Hybrid search

Combine **dense vector similarity**, **sparse learned retrieval**, and **BM25 full-text** in a single query. Results are fused with **weighted combination** — each modality contributes a score scaled by its weight, and the sums rank the final top-k.

This typically outperforms any single modality, especially for RAG.

## Why hybrid

- Dense vectors capture **semantic** similarity but miss exact term matches.
- Sparse vectors / BM25 capture **lexical** matches but miss semantics.
- Weighted fusion lets you tune the blend per query or workload.

## Two-way hybrid (dense + BM25)

```python
results = client.hybrid_search(
    "docs",
    query_vector=[0.1]*768,
    text_query="running shoes",
    text_field="description",     # metadata field to BM25-index
    top_k=10,
    vector_weight=0.6,
    text_weight=0.4,
    return_metadata=True,
)
for r in results:
    print(r.id, r.distance, r.metadata["description"])
```

`text_field` points to a metadata string column. BM25 is computed over that field with Lucene-style IDF.

## Three-way hybrid (dense + sparse + BM25)

Sparse vectors are plain `dict[int, float]` — see [sparse vectors](sparse-vectors.md).

```python
results = client.hybrid_search(
    "docs",
    query_vector=dense_query,
    sparse_query={42: 0.7, 137: 0.5},
    text_query="running shoes",
    text_field="description",
    top_k=10,
    vector_weight=0.5,
    text_weight=0.2,
    sparse_weight=0.3,
    return_metadata=True,
)
```

## Weight tuning

Weights are linear scalars, not RRF `k`. Any combination that's non-negative works; the defaults are:

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `vector_weight` | `0.5` | Dense vector contribution |
| `text_weight` | `0.5` | BM25 contribution |
| `sparse_weight` | `0.0` | Sparse contribution (off by default) |

Set any weight to `0.0` to disable that modality. You can omit the corresponding input entirely in that case.

Typical starting points:

- **Dense-heavy** (semantic): `vector=0.7, text=0.3`
- **Lexical-heavy** (exact terms): `vector=0.3, text=0.7`
- **Balanced three-way**: `vector=0.5, text=0.2, sparse=0.3`

## Filtering

Any metadata predicate works alongside hybrid search:

```python
results = client.hybrid_search(
    "docs",
    query_vector=dense_query,
    text_query="running shoes",
    text_field="description",
    filter_expression="category = 'footwear' AND in_stock = true",
    top_k=10,
)
```

See [metadata filtering](metadata-filtering.md).

## Return shape

Each `SearchResult` carries `id`, `distance` (the blended score), and optional `metadata`. There is no separate `score` field — use `distance` for both pure vector and hybrid results.

## See also

- [Sparse vectors](sparse-vectors.md) — ingesting SPLADE-style embeddings
- [RAG use case](../use-cases/rag.md) — end-to-end hybrid RAG pipeline
- [Client API — hybrid_search](../python-sdk/client.md#hybrid-search)
