# Run the GVDB CUDA smoke test suite on Modal.
#
# Example:
#   uv run --project tools/modal modal run tools/modal/gpu_test.py
#
# To switch GPU class, edit the gpu= kwarg on @app.function below. Modal 1.x
# removed Function.with_options() so per-call overrides aren't supported.

import os
import subprocess

import modal

REPO_LOCAL = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
REPO_REMOTE = "/root/repo"
BUILD_DIR = f"{REPO_REMOTE}/build"
CCACHE_DIR = "/root/.ccache"

# The CUDA build only needs the C++ sources, headers, proto, tests, and the
# Modal scripts. Heavy non-C++ subprojects are excluded so we don't upload
# Go module caches and Python environments per run.
_REPO_IGNORE = [
    "**/build/**",
    "**/build-*/**",
    "**/.git/**",
    "**/.venv/**",
    "**/__pycache__/**",
    "**/.cache/**",
    "**/.tox/**",
    "**/.pytest_cache/**",
    "**/*.pyc",
    "**/.DS_Store",
    # Never ship secrets to a remote container.
    "**/.env",
    "**/.env.*",
    "**/*.pem",
    "**/*.p12",
    "**/id_rsa",
    "**/id_rsa.*",
    "**/*.key",
    "clients/**",
    "operator/**",
    "deploy/**",
    "docs/**",
    "outputs/**",
]

# The CUDA build uses the same toolchain as the Linux CI and the release
# image: Clang 22 with libc++ (cmake/toolchains/linux-clang-libcxx.cmake).
# CUDA 13.4 and later accept it as the nvcc host compiler.
TOOLCHAIN_FILE = f"{REPO_REMOTE}/cmake/toolchains/linux-clang-libcxx.cmake"

app = modal.App("gvdb-gpu-test")

image = (
    # Ubuntu 24.04 ships cmake >= 3.28, satisfying faiss v1.8.0's requirement.
    # CUDA 13.4 is the first release that supports libc++ host code.
    modal.Image.from_registry(
        "nvidia/cuda:13.5.0-devel-ubuntu24.04",
        add_python="3.11",
    )
    .apt_install(
        "build-essential",
        "cmake",
        "ccache",
        "ninja-build",
        "git",
        "pkg-config",
        "libopenblas-dev",
        "liblapack-dev",
        "zlib1g-dev",
        "libssl-dev",
        "libcurl4-openssl-dev",
    )
    # Clang 22, libc++, lld and libomp from apt.llvm.org, with the version
    # from the toolchain file.
    .add_local_file(
        os.path.join(REPO_LOCAL, "tools/ci/install-llvm.sh"),
        "/opt/gvdb/tools/ci/install-llvm.sh",
        copy=True,
    )
    .add_local_file(
        os.path.join(REPO_LOCAL, "cmake/toolchains/linux-clang-libcxx.cmake"),
        "/opt/gvdb/cmake/toolchains/linux-clang-libcxx.cmake",
        copy=True,
    )
    .run_commands("bash /opt/gvdb/tools/ci/install-llvm.sh")
    .env({"CCACHE_DIR": CCACHE_DIR})
    .add_local_dir(REPO_LOCAL, remote_path=REPO_REMOTE, ignore=_REPO_IGNORE)
)

ccache_volume = modal.Volume.from_name("gvdb-ccache", create_if_missing=True)


# Smoke tests run fine on A10G; edit gpu= below to switch class if debugging
# a hardware-specific failure.
@app.function(
    image=image,
    volumes={CCACHE_DIR: ccache_volume},
    gpu="A10G",
    timeout=3600,
)
def run_tests(jobs: int = 8) -> None:
    def _run(cmd: list[str]) -> None:
        print(f"$ {' '.join(cmd)}", flush=True)
        subprocess.run(cmd, check=True)

    _run(
        [
            "cmake",
            "-S",
            REPO_REMOTE,
            "-B",
            BUILD_DIR,
            f"-DCMAKE_TOOLCHAIN_FILE={TOOLCHAIN_FILE}",
            "-DCMAKE_BUILD_TYPE=Release",
            "-DGVDB_WITH_CUDA=ON",
            "-DGVDB_BUILD_TESTS=ON",
            "-DCMAKE_C_COMPILER_LAUNCHER=ccache",
            "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache",
            "-DCMAKE_CUDA_COMPILER_LAUNCHER=ccache",
        ]
    )
    _run(["cmake", "--build", BUILD_DIR, "--target", "cuda_smoke_tests", "-j", str(jobs)])
    _run(["ctest", "--test-dir", BUILD_DIR, "-L", "cuda", "--output-on-failure"])


@app.local_entrypoint()
def main(jobs: int = 8) -> None:
    run_tests.remote(jobs=jobs)
