# syntax=docker/dockerfile:1

# Stage 1: Build all GVDB binaries
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libomp-dev \
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

WORKDIR /src
COPY . .

# BuildKit cache mount: persists the build dir across Docker builds so
# cmake's incremental compilation works. Dependencies (gRPC, RocksDB,
# Faiss, AWS SDK) compile once; source-only changes rebuild in ~30s.
# Binaries are copied to /out/ since the cache mount is not part of the layer.
RUN --mount=type=cache,target=/src/build \
    cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=OFF \
    -DGVDB_WITH_S3=ON \
    && cmake --build build -j4 \
    && mkdir -p /out && cp build/bin/gvdb-* /out/ && strip /out/gvdb-*

# Stage 2: Minimal runtime image
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    libomp5 \
    libgomp1 \
    libopenblas0 \
    liblapack3 \
    zlib1g \
    libcurl3t64-gnutls \
    ca-certificates \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/gvdb-* /usr/local/bin/
COPY config.example.yaml /etc/gvdb/config.yaml

RUN mkdir -p /data/gvdb

EXPOSE 50050 50051 50060 50070 8300 9050 9090 9091 9101 9102 9201
