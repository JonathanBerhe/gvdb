# Vector search

GVDB ships seven dense vector index types plus automatic selection. Choose by balancing recall, latency, and memory — or let GVDB pick for you.

## Index types

| Type | Best for | Recall | Build time | Memory |
|------|----------|--------|------------|--------|
| `FLAT` | < 10K vectors, exact search, small evaluations | 100% | Instant | Full vectors |
| `HNSW` | 10K–1M vectors, low-latency ANN | > 95% | Seconds–minutes | 1–2× vectors |
| `IVF_FLAT` | > 1M vectors, high recall, moderate memory | > 90% | Minutes | Full vectors |
| `IVF_PQ` | Billion-scale, tight memory budgets | 70–90% | Minutes | ~10–20% of raw |
| `IVF_SQ` | Large corpora with uniform distribution | 80–95% | Minutes | 25–50% of raw |
| `TurboQuant` | Extreme compression with near-optimal distortion | > 90% | Fast | 1/32 at 1-bit |
| `IVF_TURBOQUANT` | Billion-scale, sub-linear search at extreme compression | > 90% | Minutes | 7.5× reduction at 4-bit on 768D |

## AUTO selection

Set `index_type="auto"` to let GVDB pick per segment at seal time:

- **< 10K vectors** → `FLAT`
- **10K – 1M vectors** → `HNSW`
- **≥ 1M vectors** → `IVF_TURBOQUANT`

This is typically the right choice — segments in a growing collection move through phases automatically.

```python
client.create_collection("embeddings", dimension=768, index_type="auto")
```

## Distance metrics

Every index supports three metrics. Each search result has a `distance`, and a lower distance is a closer match for every metric. Results come back closest first.

| Metric | Distance | Range |
|--------|----------|-------|
| `l2` | The Euclidean distance | 0 and up |
| `ip` | Minus the inner product (dot product) | Any value |
| `cosine` | 1 minus the cosine similarity | 0 to 2 |

Every index type returns the same distance for the same pair of vectors. TurboQuant indexes return an estimate of it.

A zero vector has no direction, so a `cosine` collection rejects it. An insert, an upsert or a search with a zero vector in a `cosine` collection fails with `INVALID_ARGUMENT`. A vector with a norm below 1e-8 counts as zero.

A range search returns every vector within a limit of the query, closest first. The limit depends on the metric:

- For `l2` and `cosine`, you set `radius`, the largest distance that a result can have. It must be positive. For example, a radius of 0.2 with `cosine` returns the vectors with a cosine similarity of 0.8 or more.
- For `ip`, you set `similarity_threshold`, the smallest dot product that a result can have. A range search on an `ip` collection does not take a radius.

Any other combination fails with `INVALID_ARGUMENT`. `max_results` keeps the nearest results.

```python
client.create_collection("embeddings", dimension=768, metric="cosine")
```

## Choosing manually

### FLAT

Exact search over raw vectors. Use for datasets where a full scan fits in memory.

```python
client.create_collection("small", dimension=128, index_type="flat")
```

### HNSW

Hierarchical Navigable Small World graph. Excellent recall/latency trade-off for datasets up to ~1M vectors per segment.

```python
client.create_collection(
    "medium",
    dimension=768,
    index_type="hnsw",
    # optional tuning (defaults work well):
    # hnsw_m=16, hnsw_ef_construction=200, hnsw_ef_search=64,
)
```

### IVF_FLAT / IVF_PQ / IVF_SQ

Inverted File variants. The coarse quantizer partitions vectors into `nlist` clusters; queries probe the top `nprobe` clusters. `_PQ` and `_SQ` add compression on top.

```python
client.create_collection(
    "large",
    dimension=768,
    index_type="ivf_pq",
    # nlist=16384, nprobe=32, pq_m=96, pq_nbits=8,
)
```

### TurboQuant

Data-oblivious online quantization — see the dedicated [TurboQuant page](turboquant.md).

## SIMD and hardware acceleration

Every CPU-bound distance kernel uses runtime CPU feature detection (SSE, AVX2, AVX-512). On macOS, enable [GPU acceleration](gpu-acceleration.md) with Apple Metal for 16–24× speedup on FLAT search.

## Further reading

- [Hybrid search](hybrid-search.md) — combine dense vectors with BM25 text and sparse vectors
