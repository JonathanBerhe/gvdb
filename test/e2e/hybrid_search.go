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
	fmt.Println("GVDB Hybrid Search E2E Test")
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

	collectionName := "hybrid_e2e_test"

	// Cleanup stale data
	client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collectionName})

	// Step 1: Create collection (4D — small and deterministic)
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
	fmt.Println("✅ Created collection")

	// Step 2: Insert 5 vectors with known positions and text metadata
	// Vectors are deterministic so we know which are closest to each other
	fmt.Println("\nStep 2: Inserting vectors with text metadata...")
	type testDoc struct {
		id     uint64
		vector []float32
		text   string
	}
	docs := []testDoc{
		{1, []float32{1, 0, 0, 0}, "running shoes for marathon athletes"},
		{2, []float32{0, 1, 0, 0}, "basketball sneakers indoor courts"},
		{3, []float32{0, 0, 1, 0}, "hiking boots waterproof mountain trails"},
		{4, []float32{0, 0, 0, 1}, "running gear jogging fitness tracker"},
		{5, []float32{0.9, 0.1, 0, 0}, "casual walking shoes comfortable daily"},
	}

	vectors := make([]*pb.VectorWithId, len(docs))
	for i, d := range docs {
		vectors[i] = &pb.VectorWithId{
			Id:     d.id,
			Vector: &pb.Vector{Values: d.vector, Dimension: 4},
			Metadata: &pb.Metadata{
				Fields: map[string]*pb.MetadataValue{
					"text": {Value: &pb.MetadataValue_StringValue{StringValue: d.text}},
				},
			},
		}
	}

	_, err = client.Insert(ctx, &pb.InsertRequest{
		CollectionName: collectionName,
		Vectors:        vectors,
	})
	if err != nil {
		log.Fatalf("Insert failed: %v", err)
	}
	fmt.Printf("✅ Inserted %d vectors\n", len(docs))

	// Step 3: Hybrid search — vector close to [1,0,0,0] + text "running"
	// Expected: ID=1 ranks first (nearest vector AND contains "running")
	//           ID=4 ranks high (contains "running" but far vector)
	//           ID=5 ranks high (close vector but no "running")
	fmt.Println("\nStep 3: Hybrid search (vector [1,0,0,0] + text 'running')...")
	hybridResp, err := client.HybridSearch(ctx, &pb.HybridSearchRequest{
		CollectionName: collectionName,
		QueryVector:    &pb.Vector{Values: []float32{1, 0, 0, 0}, Dimension: 4},
		TextQuery:      "running",
		TopK:           5,
		VectorWeight:   0.5,
		TextWeight:     0.5,
		TextField:      "text",
		ReturnMetadata: true,
	})
	if err != nil {
		log.Fatalf("HybridSearch failed: %v", err)
	}
	if len(hybridResp.Results) == 0 {
		log.Fatalf("HybridSearch returned no results")
	}
	fmt.Printf("✅ Hybrid search returned %d results\n", len(hybridResp.Results))
	printResults(hybridResp.Results)

	// Verify: ID=1 should be first (best vector match + best text match)
	if hybridResp.Results[0].Id != 1 {
		log.Fatalf("Expected ID=1 first in hybrid search, got ID=%d", hybridResp.Results[0].Id)
	}
	fmt.Println("✅ ID=1 ranks first (nearest vector + 'running' text match)")

	// Step 4: Text-only search — "hiking mountain waterproof"
	// Expected: ID=3 ranks first (only doc with all three terms)
	fmt.Println("\nStep 4: Text-only search ('hiking mountain waterproof')...")
	textResp, err := client.HybridSearch(ctx, &pb.HybridSearchRequest{
		CollectionName: collectionName,
		TextQuery:      "hiking mountain waterproof",
		TopK:           3,
		VectorWeight:   0.0,
		TextWeight:     1.0,
		TextField:      "text",
		ReturnMetadata: true,
	})
	if err != nil {
		log.Fatalf("Text-only search failed: %v", err)
	}
	if len(textResp.Results) == 0 {
		log.Fatalf("Text-only search returned no results")
	}
	fmt.Printf("✅ Text-only search returned %d results\n", len(textResp.Results))
	printResults(textResp.Results)

	if textResp.Results[0].Id != 3 {
		log.Fatalf("Expected ID=3 first in text-only search, got ID=%d", textResp.Results[0].Id)
	}
	fmt.Println("✅ ID=3 ranks first (hiking boots — all query terms match)")

	// Step 5: Vector-only search — query [1,0,0,0]
	// Expected: ID=1 first (exact match), ID=5 second (closest)
	fmt.Println("\nStep 5: Vector-only search ([1,0,0,0])...")
	vecResp, err := client.Search(ctx, &pb.SearchRequest{
		CollectionName: collectionName,
		QueryVector:    &pb.Vector{Values: []float32{1, 0, 0, 0}, Dimension: 4},
		TopK:           3,
	})
	if err != nil {
		log.Fatalf("Vector-only search failed: %v", err)
	}
	if len(vecResp.Results) == 0 {
		log.Fatalf("Vector-only search returned no results")
	}
	if vecResp.Results[0].Id != 1 {
		log.Fatalf("Expected ID=1 first in vector search, got ID=%d", vecResp.Results[0].Id)
	}
	if vecResp.Results[1].Id != 5 {
		log.Fatalf("Expected ID=5 second in vector search, got ID=%d", vecResp.Results[1].Id)
	}
	fmt.Println("✅ Vector search: ID=1 first, ID=5 second (correct distance ordering)")

	// Step 6: Cleanup
	fmt.Println("\nStep 6: Cleanup...")
	_, err = client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collectionName})
	if err != nil {
		log.Fatalf("DropCollection failed: %v", err)
	}
	fmt.Println("✅ Collection dropped")

	fmt.Println("\n======================================================================")
	fmt.Println("✅ Hybrid Search E2E Test PASSED")
	fmt.Println("======================================================================")
}

func printResults(results []*pb.SearchResultEntry) {
	for i, r := range results {
		text := ""
		if r.Metadata != nil {
			if v, ok := r.Metadata.Fields["text"]; ok {
				t := v.GetStringValue()
				if len(t) > 50 {
					t = t[:50]
				}
				text = t
			}
		}
		fmt.Printf("   %d. ID=%d, score=%.4f, text=\"%s\"\n", i+1, r.Id, r.Distance, text)
	}
}

