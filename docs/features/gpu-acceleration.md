# GPU acceleration

GVDB includes a custom Metal Shading Language (MSL) kernel for flat vector distance computation. On Apple Silicon it delivers **16–24× speedup** over CPU.

!!! info "Availability"
    Apple Metal acceleration is macOS only. On Linux, NVIDIA CUDA runs FLAT and IVF search through faiss-gpu. CUDA is opt-in: build with the `cuda` preset (`-DGVDB_WITH_CUDA=ON`).

## When it fires

When you enable Metal at build time, the `IndexFactory` transparently returns a `MetalFlatIndex` whenever:

1. The build flag `GVDB_WITH_METAL=ON` is set, **and**
2. The requested index type is `FLAT`, **and**
3. A Metal-capable device is available.

No code changes. Queries simply run on the GPU.

## Build

```bash
make build PRESET=metal   # Release build with Metal, output in build/metal
```

## Benchmark

```bash
make bench-metal
```

This builds and runs `gvdb-metal-bench`, which compares CPU and GPU throughput across dimensions and batch sizes.

## Measured latency

Exhaustive FLAT L2 search, top-k=10, median of 10 runs. The Metal and CUDA benchmarks ([`test/bench/metal_bench.cpp`](https://github.com/JonathanBerhe/gvdb/blob/main/test/bench/metal_bench.cpp) and [`test/bench/cuda_bench.cpp`](https://github.com/JonathanBerhe/gvdb/blob/main/test/bench/cuda_bench.cpp)) run the same workload and sweeps, so you can compare the GPU latencies directly. Each "vs CPU" column compares with the single-query faiss CPU baseline of the same platform.

By vector count (dimension 768):

| Vectors | Metal GPU<br>(M1 Pro) | vs CPU | CUDA GPU<br>(A10G) | vs CPU |
|--------:|----------------------:|-------:|-------------------:|-------:|
| 1K      | 0.55 ms   | 5.1×  | 0.05 ms  | 6.7×  |
| 10K     | 2.40 ms   | 11.4× | 0.14 ms  | 25.7× |
| 50K     | 6.65 ms   | 20.1× | 0.50 ms  | 35.1× |
| 100K    | 12.71 ms  | 20.9× | 0.93 ms  | 35.6× |
| 500K    | 58.79 ms  | 22.7× | 3.95 ms  | 45.7× |
| 1M      | 115.23 ms | 23.1× | 8.66 ms  | 43.0× |
| 2M      | 231.70 ms | 22.8× | 17.31 ms | 40.2× |

By dimension (100K vectors):

| Dimension | Metal GPU<br>(M1 Pro) | vs CPU | CUDA GPU<br>(A10G) | vs CPU |
|----------:|----------------------:|-------:|-------------------:|-------:|
| 128       | 4.67 ms  | 9.1×  | 0.32 ms | 19.1× |
| 384       | 7.61 ms  | 17.1× | 0.66 ms | 27.5× |
| 768       | 12.14 ms | 21.6× | 0.93 ms | 40.3× |
| 1536      | 20.88 ms | 25.2× | 1.29 ms | 70.1× |

- The two "vs CPU" columns use different CPUs: the M1 Pro (NEON) and the A10G host (AVX). Do not compare the multiples across the columns. At 1M×768 the CPU baselines were 2665 ms (M1 Pro) and 372 ms (A10G host).
- You can compare the raw GPU latencies, because the workload is the same. The A10G runs FLAT about 13× faster than the integrated GPU of the M1 Pro (17.3 ms against 231.7 ms at 2M×768).
- To reproduce the Metal numbers, run `make bench-metal` on Apple Silicon. To reproduce the CUDA numbers, run `uv run --project tools/modal modal run tools/modal/gpu_bench.py`, which uses a Modal A10G.

## How it works (briefly)

- **MSL kernels** compute pairwise distances in parallel across threadgroups.
- **metal-cpp** is used to dispatch from C++ without Objective-C++ leaking outside `src/index/metal/`.
- Only FLAT is accelerated today. HNSW/IVF variants stay on the CPU because their memory access patterns are less GPU-friendly.

## Limitations

- **FLAT only** at present.
- **One GPU per node** (natural on Apple Silicon).
- **Memory-bound** at very high dimensions; fall back to IVF variants for billion-scale.

## Further reading

- [Vector search](vector-search.md): full index catalogue
