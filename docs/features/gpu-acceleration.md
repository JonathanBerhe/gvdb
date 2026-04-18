# GPU acceleration (Apple Metal)

GVDB includes a custom Metal Shading Language (MSL) kernel for flat vector distance computation. On Apple Silicon it delivers **16–24× speedup** over CPU.

!!! info "Availability"
    Apple Metal acceleration is macOS only. Linux GPU support (CUDA) is on the roadmap.

## When it fires

When you enable Metal at build time, the `IndexFactory` transparently returns a `MetalFlatIndex` whenever:

1. The build flag `GVDB_WITH_METAL=ON` is set, **and**
2. The requested index type is `FLAT`, **and**
3. A Metal-capable device is available.

No code changes. Queries simply run on the GPU.

## Build

```bash
make build CMAKE_EXTRA="-DGVDB_WITH_METAL=ON"
```

## Benchmark

```bash
make bench-metal
```

This builds and runs `gvdb-metal-bench`, which compares CPU and GPU throughput across dimensions and batch sizes.

## How it works (briefly)

- **MSL kernels** compute pairwise distances in parallel across threadgroups.
- **metal-cpp** is used to dispatch from C++ without Objective-C++ leaking outside `src/index/metal/`.
- Only FLAT is accelerated today — HNSW/IVF variants stay on the CPU because their memory access patterns are less GPU-friendly.

## Limitations

- **FLAT only** at present.
- **One GPU per node** (natural on Apple Silicon).
- **Memory-bound** at very high dimensions; fall back to IVF variants for billion-scale.

## Further reading

- [Vector search](vector-search.md) — full index catalogue
- [Architecture — modules](../architecture/modules.md) for where Metal code lives
