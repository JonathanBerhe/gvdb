#!/usr/bin/env bash
# Installs Clang, lld, libc++, libc++abi and libomp of one LLVM major version
# from apt.llvm.org on Ubuntu or Debian. CI, the release image and the CUDA
# image use it.
#
# Usage: tools/ci/install-llvm.sh [--runtime] [major-version]
#
# --runtime installs only the OpenMP runtime (libomp5) of that version, for a
# host that runs GVDB programs but does not build them. The programs link
# libc++ statically, but they need the OpenMP runtime they were built with.
#
# Without a version, the script takes GVDB_LLVM_VERSION from
# cmake/toolchains/linux-clang-libcxx.cmake, so the version is set in one
# place.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
toolchain="$script_dir/../../cmake/toolchains/linux-clang-libcxx.cmake"
runtime_only=false
if [[ "${1:-}" == "--runtime" ]]; then
  runtime_only=true
  shift
fi
version=${1:-}
if [[ -z "$version" ]]; then
  version=$(sed -n 's/^set(GVDB_LLVM_VERSION \([0-9][0-9]*\).*/\1/p' "$toolchain")
fi
if [[ -z "$version" ]]; then
  echo "install-llvm.sh: no version given and none found in $toolchain" >&2
  exit 1
fi

sudo=""
if [[ "$(id -u)" -ne 0 ]]; then sudo=sudo; fi
codename=$(. /etc/os-release && echo "$VERSION_CODENAME")

$sudo apt-get update -qq
$sudo apt-get install -y -qq --no-install-recommends ca-certificates wget gnupg
wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key \
  | $sudo tee /etc/apt/trusted.gpg.d/apt.llvm.org.asc >/dev/null
echo "deb http://apt.llvm.org/${codename}/ llvm-toolchain-${codename}-${version} main" \
  | $sudo tee "/etc/apt/sources.list.d/llvm-${version}.list" >/dev/null
$sudo apt-get update -qq
if [[ "$runtime_only" == true ]]; then
  # apt.llvm.org's libomp5 has a higher version than Ubuntu's, so apt takes it.
  $sudo apt-get install -y -qq --no-install-recommends libomp5
  dpkg-query -W libomp5
  exit 0
fi
$sudo apt-get install -y -qq --no-install-recommends \
  "clang-${version}" "lld-${version}" \
  "libc++-${version}-dev" "libc++abi-${version}-dev" "libomp-${version}-dev"
"clang++-${version}" --version | head -1
