package main

import (
	"math/rand"
	"os"
	"strconv"

	pb "gvdb/integration-tests/pb"
)

// GetServerAddr returns the GVDB server address from GVDB_SERVER_ADDR env var,
// or defaults to localhost:50051.
func GetServerAddr() string {
	if addr := os.Getenv("GVDB_SERVER_ADDR"); addr != "" {
		return addr
	}
	return "localhost:50051"
}

// LoadScale returns the multiplier applied to stress-test dataset sizes
// (batch counts, per-thread payload counts). Reads the GVDB_E2E_SCALE env
// variable; defaults to 1.0.
//
// Use case: the load test targets realistic volumes (25K × 768D, 10-thread
// concurrent inserts) that overwhelm resource-constrained local clusters
// (e.g. a single-node kind deployment). Setting GVDB_E2E_SCALE=0.2 keeps
// the same scenarios and code paths exercised, at 20% of the volume.
//
// CI and full-environment runs leave this unset, preserving current coverage.
// Values ≤ 0 or unparseable inputs fall back to 1.0.
func LoadScale() float64 {
	raw := os.Getenv("GVDB_E2E_SCALE")
	if raw == "" {
		return 1.0
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 {
		return 1.0
	}
	return v
}

// Scaled applies LoadScale to an integer count. The result is clamped to a
// minimum of 1 so scaling never fully zeroes a test case — the test is still
// meaningful at small sizes.
func Scaled(n int) int {
	s := int(float64(n) * LoadScale())
	if s < 1 {
		return 1
	}
	return s
}

// GenerateRandomVector creates a normalized random vector of the given dimension.
func GenerateRandomVector(dim uint32) *pb.Vector {
	values := make([]float32, dim)
	var sum float32
	for i := range values {
		values[i] = rand.Float32()*2 - 1
		sum += values[i] * values[i]
	}
	norm := float32(1.0 / (sum + 1e-10))
	for i := range values {
		values[i] *= norm
	}
	return &pb.Vector{Values: values, Dimension: dim}
}
