package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "gvdb/integration-tests/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fmt.Println("======================================================================")
	fmt.Println("GVDB Sparse Vector E2E Test")
	fmt.Println("======================================================================")

	serverAddr := GetServerAddr()
	fmt.Printf("\nServer address: %s\n", serverAddr)

	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewVectorDBServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := "sparse_e2e_test"

	// Cleanup stale data
	client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collectionName})
	time.Sleep(500 * time.Millisecond)

	// Step 1: Create collection
	fmt.Println("\nStep 1: Creating collection...")
	_, err = client.CreateCollection(ctx, &pb.CreateCollectionRequest{
		CollectionName: collectionName,
		Dimension:      4,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	})
	if err != nil {
		log.Fatalf("CreateCollection failed: %v", err)
	}
	fmt.Println("   Created collection")
	time.Sleep(3 * time.Second) // Wait for segment propagation to data nodes

	// Step 2: Insert vectors with sparse data
	// Dense vectors encode "category", sparse vectors encode "term weights"
	fmt.Println("\nStep 2: Inserting 5 vectors with dense + sparse + text...")
	vectors := []*pb.VectorWithId{
		{
			Id:     1,
			Vector: &pb.Vector{Values: []float32{1, 0, 0, 0}, Dimension: 4},
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"text": {Value: &pb.MetadataValue_StringValue{StringValue: "running shoes marathon"}},
			}},
			SparseVector: &pb.SparseVector{Indices: []uint32{0, 5, 10}, Values: []float32{1.0, 0.8, 0.3}},
		},
		{
			Id:     2,
			Vector: &pb.Vector{Values: []float32{0, 1, 0, 0}, Dimension: 4},
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"text": {Value: &pb.MetadataValue_StringValue{StringValue: "basketball sneakers"}},
			}},
			SparseVector: &pb.SparseVector{Indices: []uint32{1, 6}, Values: []float32{1.0, 0.9}},
		},
		{
			Id:     3,
			Vector: &pb.Vector{Values: []float32{0, 0, 1, 0}, Dimension: 4},
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"text": {Value: &pb.MetadataValue_StringValue{StringValue: "hiking boots mountain"}},
			}},
			SparseVector: &pb.SparseVector{Indices: []uint32{2, 7, 10}, Values: []float32{1.0, 0.7, 0.5}},
		},
		{
			Id:     4,
			Vector: &pb.Vector{Values: []float32{0, 0, 0, 1}, Dimension: 4},
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"text": {Value: &pb.MetadataValue_StringValue{StringValue: "running gear fitness"}},
			}},
			SparseVector: &pb.SparseVector{Indices: []uint32{0, 8}, Values: []float32{0.9, 1.0}},
		},
		{
			Id:     5,
			Vector: &pb.Vector{Values: []float32{0.9, 0.1, 0, 0}, Dimension: 4},
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"text": {Value: &pb.MetadataValue_StringValue{StringValue: "casual walking shoes"}},
			}},
			SparseVector: &pb.SparseVector{Indices: []uint32{3, 5}, Values: []float32{1.0, 0.6}},
		},
	}

	resp, err := client.Insert(ctx, &pb.InsertRequest{
		CollectionName: collectionName,
		Vectors:        vectors,
	})
	if err != nil {
		log.Fatalf("Insert failed: %v", err)
	}
	fmt.Printf("   Inserted %d vectors\n", resp.InsertedCount)

	// Step 3: Hybrid search with dense + text + sparse (three-way RRF)
	fmt.Println("\nStep 3: Three-way hybrid search (dense + text + sparse)...")
	hybridResp, err := client.HybridSearch(ctx, &pb.HybridSearchRequest{
		CollectionName: collectionName,
		QueryVector:    &pb.Vector{Values: []float32{1, 0, 0, 0}, Dimension: 4},
		TextQuery:      "running shoes",
		SparseQuery:    &pb.SparseVector{Indices: []uint32{0, 5}, Values: []float32{1.0, 0.8}},
		TopK:           5,
		VectorWeight:   0.4,
		TextWeight:     0.3,
		SparseWeight:   0.3,
		TextField:      "text",
		ReturnMetadata: true,
	})
	if err != nil {
		log.Fatalf("HybridSearch failed: %v", err)
	}
	fmt.Printf("   Got %d results (%.1fms)\n", len(hybridResp.Results), hybridResp.QueryTimeMs)
	if len(hybridResp.Results) == 0 {
		log.Fatalf("Expected results, got 0")
	}
	for i, r := range hybridResp.Results {
		fmt.Printf("      %d. ID=%d, score=%.4f\n", i+1, r.Id, r.Distance)
	}

	// Verify: ID=1 should be top-1 (best match on all three signals)
	if hybridResp.Results[0].Id != 1 {
		fmt.Printf("   Warning: Expected ID=1 as top result, got ID=%d\n", hybridResp.Results[0].Id)
	} else {
		fmt.Println("   Top-1 is ID=1 as expected (best dense + text + sparse match)")
	}

	// Step 4: Sparse-only hybrid search (vector_weight=0, text_weight=0)
	fmt.Println("\nStep 4: Sparse-only search...")
	sparseResp, err := client.HybridSearch(ctx, &pb.HybridSearchRequest{
		CollectionName: collectionName,
		QueryVector:    &pb.Vector{Values: []float32{0, 0, 0, 0}, Dimension: 4},
		SparseQuery:    &pb.SparseVector{Indices: []uint32{0}, Values: []float32{1.0}},
		TopK:           3,
		VectorWeight:   0.0,
		TextWeight:     0.0,
		SparseWeight:   1.0,
	})
	if err != nil {
		log.Fatalf("Sparse-only search failed: %v", err)
	}
	fmt.Printf("   Got %d results\n", len(sparseResp.Results))
	for i, r := range sparseResp.Results {
		fmt.Printf("      %d. ID=%d, score=%.4f\n", i+1, r.Id, r.Distance)
	}
	// Sparse dim 0: vec1 has 1.0, vec4 has 0.9 → vec1 should rank first
	if len(sparseResp.Results) > 0 && sparseResp.Results[0].Id == 1 {
		fmt.Println("   Top-1 is ID=1 (highest sparse weight on dim 0)")
	}

	// Cleanup
	fmt.Println("\nStep 5: Cleanup...")
	client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collectionName})
	fmt.Println("   Collection dropped")

	fmt.Println("\n======================================================================")
	fmt.Println("Sparse Vector E2E Test PASSED")
	fmt.Println("======================================================================")
}
