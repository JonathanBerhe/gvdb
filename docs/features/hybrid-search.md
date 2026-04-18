# Hybrid search

Combine **dense vector similarity**, **sparse learned retrieval**, and **BM25 full-text** in a single query. Results are fused with **Reciprocal Rank Fusion (RRF)**.

This typically outperforms any single modality, especially for RAG.

## Why hybrid

- Dense vectors capture **semantic** similarity but miss exact term matches.
- Sparse vectors / BM25 capture **lexical** matches but miss semantics.
- RRF blends the ranks without tuning per-query weights.

## Two-way hybrid (dense + BM25)

```python
results = client.hybrid_search(
    "docs",
    query_vector=[0.1, 0.2, ...],   # dense embedding
    text_query="running shoes",
    top_k=10,
    text_field="description",        # metadata field to BM25-index
    return_metadata=True,
)
for r in results:
    print(r.id, r.score, r.metadata["description"])
```

`text_field` points to a metadata string column. BM25 is computed over that field with Lucene-style IDF.

## Three-way hybrid (dense + sparse + BM25)

```python
from gvdb import SparseVector

results = client.hybrid_search(
    "docs",
    query_vector=dense_query,
    sparse_vector=SparseVector({42: 0.7, 137: 0.5}),
    text_query="running shoes",
    text_field="description",
    top_k=10,
    return_metadata=True,
)
```

## RRF tuning

Reciprocal Rank Fusion combines lists by:

`score(d) = Σ_i 1 / (k + rank_i(d))`

The default `k = 60` rewards high-ranked items without over-weighting a single source. Override with the `rrf_k` parameter if needed.

## Filtering

Any metadata predicate works alongside hybrid search:

```python
results = client.hybrid_search(
    "docs",
    query_vector=dense_query,
    text_query="running shoes",
    filter_expression="category = 'footwear' AND in_stock = true",
    top_k=10,
)
```

See [metadata filtering](metadata-filtering.md).

## Further reading

- [Sparse vectors](sparse-vectors.md) — ingesting SPLADE-style embeddings
- [RAG use case](../use-cases/rag.md) — end-to-end hybrid RAG pipeline
