// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Metal compute kernels for vector distance computation.
// Each thread computes the distance between one query and one database vector.

#include <metal_stdlib>
using namespace metal;

// L2 squared distance: ||q - v||^2
kernel void l2_distance(
    device const float* queries    [[buffer(0)]],   // [nq * dim]
    device const float* vectors    [[buffer(1)]],   // [nb * dim]
    device float* distances        [[buffer(2)]],   // [nq * nb]
    constant uint& dim             [[buffer(3)]],
    constant uint& nb              [[buffer(4)]],
    uint2 gid                      [[thread_position_in_grid]])
{
    uint q = gid.x;  // query index
    uint v = gid.y;  // vector index

    float dist = 0.0f;
    for (uint d = 0; d < dim; d++) {
        float diff = queries[q * dim + d] - vectors[v * dim + d];
        dist += diff * diff;
    }
    distances[q * nb + v] = dist;
}

// Inner product: q . v
kernel void inner_product(
    device const float* queries    [[buffer(0)]],
    device const float* vectors    [[buffer(1)]],
    device float* distances        [[buffer(2)]],
    constant uint& dim             [[buffer(3)]],
    constant uint& nb              [[buffer(4)]],
    uint2 gid                      [[thread_position_in_grid]])
{
    uint q = gid.x;
    uint v = gid.y;

    float dot = 0.0f;
    for (uint d = 0; d < dim; d++) {
        dot += queries[q * dim + d] * vectors[v * dim + d];
    }
    // Store raw positive dot product (matches faiss output convention).
    // TopK sorts descending for IP to find most similar.
    distances[q * nb + v] = dot;
}

// Cosine distance: 1 - (q . v) / (||q|| * ||v||)
kernel void cosine_distance(
    device const float* queries    [[buffer(0)]],
    device const float* vectors    [[buffer(1)]],
    device float* distances        [[buffer(2)]],
    constant uint& dim             [[buffer(3)]],
    constant uint& nb              [[buffer(4)]],
    uint2 gid                      [[thread_position_in_grid]])
{
    uint q = gid.x;
    uint v = gid.y;

    float dot = 0.0f;
    float norm_q = 0.0f;
    float norm_v = 0.0f;

    for (uint d = 0; d < dim; d++) {
        float qval = queries[q * dim + d];
        float vval = vectors[v * dim + d];
        dot += qval * vval;
        norm_q += qval * qval;
        norm_v += vval * vval;
    }

    float denom = sqrt(norm_q) * sqrt(norm_v);
    float cosine_sim = (denom > 0.0f) ? (dot / denom) : 0.0f;
    distances[q * nb + v] = 1.0f - cosine_sim;
}
