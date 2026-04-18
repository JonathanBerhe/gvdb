# TurboQuant

**Data-oblivious online quantization with near-optimal distortion** (ICLR 2026).

TurboQuant compresses vectors to 1, 2, 4, or 8 bits per dimension without a training phase. At 4-bit on 768-dimensional embeddings, it achieves a **7.5× memory reduction** while preserving > 90% recall.

## When to use

- **Extreme memory pressure**: 10× or more vectors than you can fit in RAM at full precision
- **Billion-scale collections** where IVF_TURBOQUANT enables sub-linear search
- **Cost-sensitive deployments**: fewer replicas, smaller nodes

## Index types

### `TurboQuant`

Pure quantization over a flat scan. Compresses vectors but still evaluates every one.

```python
client.create_collection(
    "compressed",
    dimension=768,
    index_type="turboquant",
    # bits=4,  # 1, 2, 4, or 8
)
```

### `IVF_TURBOQUANT`

Combines IVF partitioning with TurboQuant. The coarse quantizer skips irrelevant clusters; TurboQuant compresses what's scanned. This is the **default choice for `AUTO` at ≥ 1M vectors**.

```python
client.create_collection(
    "billion_scale",
    dimension=768,
    index_type="ivf_turboquant",
)
```

## Compression vs recall

Rough guidance at dimension 768:

| Bits | Memory reduction | Typical recall@10 |
|------|------------------|-------------------|
| 8 | 4× | > 97% |
| 4 | 7.5× | > 90% |
| 2 | 15× | 85–90% |
| 1 | 30× | 75–85% |

Tune per workload — recall depends on data distribution.

## How it works (briefly)

TurboQuant uses a **data-oblivious** randomized projection followed by per-dimension scalar quantization. Unlike product quantization, there is no codebook to train; quantization parameters are derived from the input vector at insert time. The distortion bound is near-optimal (within a constant factor of the information-theoretic lower bound for the given bit budget).

The reference: *TurboQuant: Online Near-Optimal Quantization for Vectors* (ICLR 2026).

## Further reading

- [Vector search](vector-search.md) — full index catalogue
