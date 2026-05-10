# Modal app definition shared by gpu_bench.py and gpu_test.py.
# Provides the CUDA dev image, ccache volume, and the _build() helper that
# configures CMake with -DGVDB_WITH_CUDA=ON and links against the cached
# Faiss FetchContent tree.

import subprocess

import modal

REPO_REMOTE = "/root/repo"
BUILD_DIR = f"{REPO_REMOTE}/build"
CCACHE_DIR = "/root/.ccache"

app = modal.App("gvdb-gpu")

image = (
    modal.Image.from_registry(
        "nvidia/cuda:12.6.2-devel-ubuntu22.04",
        add_python="3.11",
    )
    .apt_install(
        "build-essential",
        "cmake",
        "ccache",
        "ninja-build",
        "git",
        "pkg-config",
        "libomp-dev",
        "libopenblas-dev",
        "liblapack-dev",
        "zlib1g-dev",
        "libssl-dev",
        "libcurl4-openssl-dev",
    )
    .env({"CCACHE_DIR": CCACHE_DIR})
)

ccache_volume = modal.Volume.from_name("gvdb-ccache", create_if_missing=True)


def _run(cmd: list[str], cwd: str | None = None) -> None:
    print(f"$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=cwd, check=True)


def build_cuda(jobs: int) -> None:
    """Configure + build with -DGVDB_WITH_CUDA=ON. Idempotent; ccache and
    the FetchContent tree are persisted in the volume so warm rebuilds are
    incremental."""
    _run(
        [
            "cmake",
            "-S",
            REPO_REMOTE,
            "-B",
            BUILD_DIR,
            "-DCMAKE_BUILD_TYPE=Release",
            "-DGVDB_WITH_CUDA=ON",
            "-DBUILD_TESTING=ON",
            "-DCMAKE_C_COMPILER_LAUNCHER=ccache",
            "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache",
            "-DCMAKE_CUDA_COMPILER_LAUNCHER=ccache",
        ]
    )
    _run(["cmake", "--build", BUILD_DIR, "-j", str(jobs)])
