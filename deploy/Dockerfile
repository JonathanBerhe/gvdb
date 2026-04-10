# Stage 1: Build all GVDB binaries
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libomp-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libopenblas-dev \
    liblapack-dev \
    zlib1g-dev \
    pkg-config \
    autoconf \
    libtool \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=OFF \
    -DGVDB_WITH_S3=ON \
    && cmake --build build -j4 \
    && strip build/bin/gvdb-*

# Stage 2: Minimal runtime image
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    libomp5 \
    libgomp1 \
    libopenblas0 \
    liblapack3 \
    zlib1g \
    libcurl4 \
    libssl3 \
    ca-certificates \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/bin/gvdb-* /usr/local/bin/
COPY config.example.yaml /etc/gvdb/config.yaml

RUN mkdir -p /data/gvdb

EXPOSE 50050 50051 50060 50070 8300 9050 9090 9091 9101 9102 9201
