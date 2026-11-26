# syntax=docker/dockerfile:1

# Stage 1: Build all GVDB binaries
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    git \
    libcurl4-gnutls-dev \
    libopenblas-dev \
    liblapack-dev \
    zlib1g-dev \
    pkg-config \
    autoconf \
    libtool \
    && rm -rf /var/lib/apt/lists/*
# Note: libcurl4-gnutls-dev (not -openssl-dev) avoids installing libssl-dev,
# whose OpenSSL 3.x headers would shadow the aws-lc headers that s2n-tls needs.

# Clang 22, libc++, lld and libomp from apt.llvm.org. This layer changes only
# when the LLVM version in the toolchain file changes.
COPY tools/ci/install-llvm.sh /src/tools/ci/install-llvm.sh
COPY cmake/toolchains/linux-clang-libcxx.cmake /src/cmake/toolchains/linux-clang-libcxx.cmake
RUN /src/tools/ci/install-llvm.sh && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# BuildKit cache mount: persists the build dir across Docker builds so
# cmake's incremental compilation works. Dependencies (gRPC, RocksDB,
# Faiss, AWS SDK) compile once; source-only changes rebuild in ~30s.
# Binaries are copied to /out/ since the cache mount is not part of the layer.
# Four parallel jobs keep the compiler within the memory of a default
# Docker Desktop VM.
# The release-linux preset links libc++ statically, so the runtime image needs
# no C++ standard library. The OpenMP runtime stays a shared library, and the
# runtime stage copies the one the binaries were built against.
RUN --mount=type=cache,target=/src/build \
    cmake --preset release-linux \
    && cmake --build --preset release-linux --parallel 4 \
    && mkdir -p /out/lib && cp build/release-linux/bin/gvdb-* /out/ && strip /out/gvdb-* \
    && cp "$(readlink -f /usr/lib/$(uname -m)-linux-gnu/libomp.so.5)" /out/lib/libomp.so.5

# Stage 2: Minimal runtime image
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    libopenblas0 \
    liblapack3 \
    zlib1g \
    libcurl3t64-gnutls \
    ca-certificates \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/gvdb-* /usr/local/bin/
COPY --from=builder /out/lib/libomp.so.5 /usr/local/lib/libomp.so.5
RUN ldconfig
COPY config.example.yaml /etc/gvdb/config.yaml

RUN mkdir -p /data/gvdb

EXPOSE 50050 50051 50060 50070 8300 9050 9090 9091 9101 9102 9201
