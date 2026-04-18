# Sparse vectors

GVDB supports sparse retrieval for **learned sparse embeddings** like SPLADE — where most dimensions are zero and a small subset carry the signal.

## When to use

- **Neural sparse retrieval**: SPLADE, uniCOIL, TILDE, etc.
- **Lexical-semantic hybrid**: combine sparse (term-aware) with dense (semantic) in a single query — see [hybrid search](hybrid-search.md).

## How it works

Sparse vectors are stored in an **inverted posting-list index**. For each non-zero dimension, GVDB keeps a posting list sorted by weight. Query evaluation uses WAND-style top-k with dynamic pruning.

## Insert

In the Python SDK, sparse vectors are plain `dict[int, float]` — there is **no `SparseVector` class**. Pass them alongside dense vectors (or on their own) through `sparse_vectors=` on `insert`:

```python
from gvdb import GVDBClient

client = GVDBClient("localhost:50051")
client.create_collection("docs", dimension=768, metric="cosine")

# Mix dense + sparse in a single insert
client.insert(
    "docs",
    ids=[1, 2, 3],
    vectors=[dense_emb_1, dense_emb_2, dense_emb_3],
    sparse_vectors=[
        {42: 0.8, 137: 0.3, 2048: 1.2},
        {7: 0.5, 42: 0.9},
        {137: 1.1, 9999: 0.4},
    ],
)
```

## Search

Sparse-only search is not exposed as a dedicated method in the Python SDK. Use `hybrid_search` with `sparse_weight=1.0` and `vector_weight=0.0` to effectively do sparse-only retrieval:

```python
results = client.hybrid_search(
    "docs",
    sparse_query={42: 0.7, 137: 0.5},
    top_k=10,
    vector_weight=0.0,
    text_weight=0.0,
    sparse_weight=1.0,
)
```

## Three-way hybrid

Sparse shines in combination with dense vectors and BM25 full-text via weighted fusion. See [hybrid search](hybrid-search.md).

## See also

- [Hybrid search](hybrid-search.md) — dense + sparse + BM25 in one query
- [RAG use case](../use-cases/rag.md) — sparse-dense hybrid for retrieval-augmented generation
- [Client API — insert](../python-sdk/client.md#insert)
