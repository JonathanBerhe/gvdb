# Run the GVDB CUDA bench on Modal.
#
# Examples:
#   modal run tools/modal/gpu_bench.py
#   modal run tools/modal/gpu_bench.py --gpu A100
#   modal run tools/modal/gpu_bench.py --gpu H100

import os

import modal

from app import (
    BUILD_DIR,
    REPO_REMOTE,
    app,
    build_cuda,
    ccache_volume,
    image,
)

REPO_LOCAL = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


@app.function(
    image=image,
    volumes={"/root/.ccache": ccache_volume},
    mounts=[modal.Mount.from_local_dir(REPO_LOCAL, remote_path=REPO_REMOTE)],
    timeout=3600,
)
def bench_cuda(jobs: int = 8) -> None:
    import subprocess

    build_cuda(jobs=jobs)
    binary = f"{BUILD_DIR}/bin/gvdb-cuda-bench"
    print(f"$ {binary}", flush=True)
    subprocess.run([binary], check=True)


@app.local_entrypoint()
def main(gpu: str = "A10G", jobs: int = 8) -> None:
    """Default A10G keeps iteration cheap; override --gpu A100 / H100 for real
    perf numbers."""
    bench_cuda.with_options(gpu=gpu).remote(jobs=jobs)
