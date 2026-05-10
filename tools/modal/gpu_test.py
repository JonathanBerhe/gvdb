# Run the GVDB CUDA smoke test suite on Modal.
#
# Example:
#   modal run tools/modal/gpu_test.py

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
def run_tests(jobs: int = 8) -> None:
    import subprocess

    build_cuda(jobs=jobs)
    print("$ ctest -L cuda --output-on-failure", flush=True)
    subprocess.run(
        [
            "ctest",
            "--test-dir",
            BUILD_DIR,
            "-L",
            "cuda",
            "--output-on-failure",
        ],
        check=True,
    )


@app.local_entrypoint()
def main(gpu: str = "A10G", jobs: int = 8) -> None:
    """Smoke tests run fine on A10G; only switch to A100/H100 if debugging
    a hardware-specific failure."""
    run_tests.with_options(gpu=gpu).remote(jobs=jobs)
