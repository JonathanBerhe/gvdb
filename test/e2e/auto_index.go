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
	fmt.Println("GVDB Auto-Index Selection E2E Test")
	fmt.Println("======================================================================")

	serverAddr := GetServerAddr()
	fmt.Printf("\nServer address: %s\n", serverAddr)

	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewVectorDBServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Step 1: Create collection with AUTO index type
	collectionName := "auto_index_e2e_test"
	client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collectionName})
	time.Sleep(500 * time.Millisecond)

	fmt.Println("\nStep 1: Creating collection with AUTO index type...")
	createResp, err := client.CreateCollection(ctx, &pb.CreateCollectionRequest{
		CollectionName: collectionName,
		Dimension:      128,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_AUTO,
	})
	if err != nil {
		log.Fatalf("CreateCollection with AUTO failed: %v", err)
	}
	fmt.Printf("   Created collection '%s' (ID: %d)\n", collectionName, createResp.CollectionId)

	// Step 2: Insert vectors (<10K, so AUTO should resolve to FLAT)
	fmt.Println("\nStep 2: Inserting 500 vectors (AUTO should pick FLAT for <10K)...")
	vectors := make([]*pb.VectorWithId, 500)
	for i := 0; i < 500; i++ {
		vectors[i] = &pb.VectorWithId{
			Id:     uint64(i + 1),
			Vector: GenerateRandomVector(128),
		}
	}
	insertResp, err := client.Insert(ctx, &pb.InsertRequest{
		CollectionName: collectionName,
		Vectors:        vectors,
	})
	if err != nil {
		log.Fatalf("Insert failed: %v", err)
	}
	fmt.Printf("   Inserted %d vectors\n", insertResp.InsertedCount)

	// Step 3: Search — verifies the index was built and works
	fmt.Println("\nStep 3: Searching (verifies AUTO index works)...")
	searchResp, err := client.Search(ctx, &pb.SearchRequest{
		CollectionName: collectionName,
		QueryVector:    GenerateRandomVector(128),
		TopK:           10,
	})
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("   Found %d results (query time: %.2fms)\n",
		len(searchResp.Results), searchResp.QueryTimeMs)
	if len(searchResp.Results) == 0 {
		log.Fatalf("Expected search results, got 0")
	}
	if len(searchResp.Results) > 10 {
		log.Fatalf("Expected at most 10 results, got %d", len(searchResp.Results))
	}

	// Step 4: Verify Get works
	fmt.Println("\nStep 4: Get by ID...")
	getResp, err := client.Get(ctx, &pb.GetRequest{
		CollectionName: collectionName,
		Ids:            []uint64{1, 2, 3, 999999},
	})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("   Found %d vectors, not found %d IDs\n",
		len(getResp.Vectors), len(getResp.NotFoundIds))
	if len(getResp.Vectors) != 3 {
		log.Fatalf("Expected 3 found vectors, got %d", len(getResp.Vectors))
	}
	if len(getResp.NotFoundIds) != 1 {
		log.Fatalf("Expected 1 not-found ID, got %d", len(getResp.NotFoundIds))
	}

	// Step 5: Upsert works with AUTO
	fmt.Println("\nStep 5: Upsert (insert + update)...")
	upsertVecs := []*pb.VectorWithId{
		{Id: 1, Vector: GenerateRandomVector(128)},   // update existing
		{Id: 999, Vector: GenerateRandomVector(128)},  // insert new
	}
	upsertResp, err := client.Upsert(ctx, &pb.UpsertRequest{
		CollectionName: collectionName,
		Vectors:        upsertVecs,
	})
	if err != nil {
		log.Fatalf("Upsert failed: %v", err)
	}
	fmt.Printf("   Upserted %d (inserted: %d, updated: %d)\n",
		upsertResp.UpsertedCount, upsertResp.InsertedCount, upsertResp.UpdatedCount)

	// Step 6: Verify explicit index types still work alongside AUTO
	fmt.Println("\nStep 6: Verify explicit index type (HNSW) still works...")
	explicitCollection := "auto_index_explicit_test"
	client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: explicitCollection})
	time.Sleep(500 * time.Millisecond)

	_, err = client.CreateCollection(ctx, &pb.CreateCollectionRequest{
		CollectionName: explicitCollection,
		Dimension:      64,
		Metric:         pb.CreateCollectionRequest_COSINE,
		IndexType:      pb.CreateCollectionRequest_HNSW,
	})
	if err != nil {
		log.Fatalf("CreateCollection with HNSW failed: %v", err)
	}

	explicitVecs := make([]*pb.VectorWithId, 100)
	for i := 0; i < 100; i++ {
		explicitVecs[i] = &pb.VectorWithId{
			Id:     uint64(i + 1),
			Vector: GenerateRandomVector(64),
		}
	}
	_, err = client.Insert(ctx, &pb.InsertRequest{
		CollectionName: explicitCollection,
		Vectors:        explicitVecs,
	})
	if err != nil {
		log.Fatalf("Insert into HNSW collection failed: %v", err)
	}

	explicitSearch, err := client.Search(ctx, &pb.SearchRequest{
		CollectionName: explicitCollection,
		QueryVector:    GenerateRandomVector(64),
		TopK:           5,
	})
	if err != nil {
		log.Fatalf("Search HNSW collection failed: %v", err)
	}
	fmt.Printf("   HNSW collection: found %d results\n", len(explicitSearch.Results))
	if len(explicitSearch.Results) == 0 {
		log.Fatalf("Expected HNSW search results, got 0")
	}

	// Step 7: List collections — both should appear
	fmt.Println("\nStep 7: List collections...")
	listResp, err := client.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		log.Fatalf("ListCollections failed: %v", err)
	}
	fmt.Printf("   Found %d collections\n", len(listResp.Collections))
	for _, c := range listResp.Collections {
		fmt.Printf("      - %s (ID: %d, dim: %d, vectors: %d)\n",
			c.CollectionName, c.CollectionId, c.Dimension, c.VectorCount)
	}

	// Cleanup
	fmt.Println("\nStep 8: Cleanup...")
	client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collectionName})
	client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: explicitCollection})
	fmt.Println("   Collections dropped")

	fmt.Println("\n======================================================================")
	fmt.Println("Auto-Index Selection E2E Test PASSED")
	fmt.Println("======================================================================")
}
