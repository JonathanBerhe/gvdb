package main

import (
	"math/rand"
	"os"

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
