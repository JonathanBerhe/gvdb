# Sparse vectors

GVDB supports sparse retrieval for **learned sparse embeddings** like SPLADE — where most dimensions are zero and a small subset carry the signal.

## When to use

- **Neural sparse retrieval**: SPLADE, uniCOIL, TILDE, etc.
- **Lexical-semantic hybrid**: combine sparse (term-aware) with dense (semantic) in a single query — see [hybrid search](hybrid-search.md).

## How it works

Sparse vectors are stored in an **inverted posting-list index**. For each non-zero dimension, GVDB keeps a posting list of `(vector_id, weight)` pairs sorted by weight. Query evaluation uses WAND-style top-k with dynamic pruning.

## Insert

Use `SparseVector` with a dict of `{dim: weight}`:

```python
from gvdb import GVDBClient, SparseVector

client = GVDBClient("localhost:50051")
client.create_collection_sparse("sparse_col")

client.insert_sparse(
    "sparse_col",
    ids=[1, 2, 3],
    sparse_vectors=[
        SparseVector({42: 0.8, 137: 0.3, 2048: 1.2}),
        SparseVector({7: 0.5, 42: 0.9}),
        SparseVector({137: 1.1, 9999: 0.4}),
    ],
)
```

## Search

```python
query = SparseVector({42: 0.7, 137: 0.5})
results = client.search_sparse("sparse_col", query, top_k=10)
for r in results:
    print(r.id, r.distance)
```

## Three-way hybrid

Sparse shines in combination with dense vectors and BM25 full-text via Reciprocal Rank Fusion. See [hybrid search](hybrid-search.md).

## Further reading

- [Hybrid search](hybrid-search.md) — dense + sparse + BM25 in one query
- [RAG use case](../use-cases/rag.md) — sparse-dense hybrid for retrieval-augmented generation
