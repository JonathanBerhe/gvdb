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
	serverAddr := GetServerAddr()
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewVectorDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	fmt.Println("========================================")
	fmt.Println("Comprehensive DropCollection E2E Test")
	fmt.Println("========================================\n")

	// Test 1: Create multiple collections of different types
	fmt.Println("Test 1: Creating multiple collections...")
	collections := []struct {
		name      string
		dimension uint32
		metric    pb.CreateCollectionRequest_MetricType
		index     pb.CreateCollectionRequest_IndexType
	}{
		{"test_flat_1", 128, pb.CreateCollectionRequest_L2, pb.CreateCollectionRequest_FLAT},
		{"test_flat_2", 256, pb.CreateCollectionRequest_COSINE, pb.CreateCollectionRequest_FLAT},
		{"test_hnsw_1", 384, pb.CreateCollectionRequest_L2, pb.CreateCollectionRequest_HNSW},
		{"test_ivf_pq_1", 512, pb.CreateCollectionRequest_INNER_PRODUCT, pb.CreateCollectionRequest_IVF_PQ},
		{"test_ivf_flat_1", 768, pb.CreateCollectionRequest_L2, pb.CreateCollectionRequest_IVF_FLAT},
	}

	collectionIds := make(map[string]uint32)
	for _, coll := range collections {
		createReq := &pb.CreateCollectionRequest{
			CollectionName: coll.name,
			Dimension:      coll.dimension,
			Metric:         coll.metric,
			IndexType:      coll.index,
		}
		resp, err := client.CreateCollection(ctx, createReq)
		if err != nil {
			log.Fatalf("Failed to create collection %s: %v", coll.name, err)
		}
		collectionIds[coll.name] = resp.CollectionId
		fmt.Printf("  ✅ Created %s (ID: %d, dim: %d, index: %s)\n",
			coll.name, resp.CollectionId, coll.dimension, coll.index)
	}

	// Test 2: List all collections
	fmt.Println("\nTest 2: Listing all collections...")
	listResp, err := client.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		log.Fatalf("Failed to list collections: %v", err)
	}
	if len(listResp.Collections) != 5 {
		log.Fatalf("Expected 5 collections, got %d", len(listResp.Collections))
	}
	fmt.Printf("  ✅ Listed %d collections\n", len(listResp.Collections))

	// Test 3: Drop specific collections (alternating)
	fmt.Println("\nTest 3: Dropping alternating collections...")
	toDrop := []string{"test_flat_1", "test_hnsw_1", "test_ivf_flat_1"}
	for _, name := range toDrop {
		dropReq := &pb.DropCollectionRequest{CollectionName: name}
		_, err := client.DropCollection(ctx, dropReq)
		if err != nil {
			log.Fatalf("Failed to drop collection %s: %v", name, err)
		}
		fmt.Printf("  ✅ Dropped %s\n", name)
	}

	// Test 4: Verify remaining collections still exist
	fmt.Println("\nTest 4: Verifying remaining collections...")
	listResp2, err := client.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		log.Fatalf("Failed to list collections: %v", err)
	}
	if len(listResp2.Collections) != 2 {
		log.Fatalf("Expected 2 collections after drops, got %d", len(listResp2.Collections))
	}
	remaining := make(map[string]bool)
	for _, coll := range listResp2.Collections {
		remaining[coll.CollectionName] = true
	}
	if !remaining["test_flat_2"] || !remaining["test_ivf_pq_1"] {
		log.Fatalf("Wrong collections remaining: %v", listResp2.Collections)
	}
	fmt.Printf("  ✅ Correct 2 collections remain: test_flat_2, test_ivf_pq_1\n")

	// Test 5: Try to drop non-existent collection
	fmt.Println("\nTest 5: Attempting to drop non-existent collection...")
	dropReq := &pb.DropCollectionRequest{CollectionName: "nonexistent_collection"}
	_, err = client.DropCollection(ctx, dropReq)
	if err == nil {
		log.Fatalf("Expected error when dropping non-existent collection, got none")
	}
	fmt.Printf("  ✅ Correctly failed with error: %v\n", err)

	// Test 6: Recreate dropped collection with same name but different params
	fmt.Println("\nTest 6: Recreating dropped collection with different params...")
	originalId := collectionIds["test_flat_1"]
	createReq := &pb.CreateCollectionRequest{
		CollectionName: "test_flat_1",
		Dimension:      1024, // Different dimension
		Metric:         pb.CreateCollectionRequest_COSINE,  // Different metric
		IndexType:      pb.CreateCollectionRequest_HNSW,    // Different index
	}
	resp, err := client.CreateCollection(ctx, createReq)
	if err != nil {
		log.Fatalf("Failed to recreate test_flat_1: %v", err)
	}
	newId := resp.CollectionId
	if newId == originalId {
		log.Fatalf("Recreated collection has same ID as original (%d), expected different", newId)
	}
	fmt.Printf("  ✅ Recreated test_flat_1 with new ID %d (original was %d)\n", newId, originalId)

	// Test 7: Insert vectors into remaining and recreated collections
	fmt.Println("\nTest 7: Inserting vectors into collections...")
	testCollections := []string{"test_flat_2", "test_ivf_pq_1", "test_flat_1"}
	for _, name := range testCollections {
		// Get collection metadata to know dimension
		listResp, err := client.ListCollections(ctx, &pb.ListCollectionsRequest{})
		if err != nil {
			log.Fatalf("Failed to list collections: %v", err)
		}
		var dim uint32
		for _, coll := range listResp.Collections {
			if coll.CollectionName == name {
				dim = coll.Dimension
				break
			}
		}

		// Create vector with correct dimension
		vector := make([]float32, dim)
		for i := range vector {
			vector[i] = float32(i) * 0.1
		}

		insertReq := &pb.InsertRequest{
			CollectionName: name,
			Vectors: []*pb.VectorWithId{
				{Id: 1, Vector: &pb.Vector{Values: vector, Dimension: dim}},
				{Id: 2, Vector: &pb.Vector{Values: vector, Dimension: dim}},
			},
		}
		_, err = client.Insert(ctx, insertReq)
		if err != nil {
			log.Fatalf("Failed to insert into %s: %v", name, err)
		}
		fmt.Printf("  ✅ Inserted 2 vectors into %s (dim: %d)\n", name, dim)
	}

	// Test 8: Search in collections to verify they work
	fmt.Println("\nTest 8: Searching in collections...")
	for _, name := range testCollections {
		// Get dimension
		listResp, err := client.ListCollections(ctx, &pb.ListCollectionsRequest{})
		if err != nil {
			log.Fatalf("Failed to list collections: %v", err)
		}
		var dim uint32
		for _, coll := range listResp.Collections {
			if coll.CollectionName == name {
				dim = coll.Dimension
				break
			}
		}

		query := make([]float32, dim)
		for i := range query {
			query[i] = float32(i) * 0.1
		}

		searchReq := &pb.SearchRequest{
			CollectionName: name,
			QueryVector:    &pb.Vector{Values: query, Dimension: dim},
			TopK:           5,
		}
		searchResp, err := client.Search(ctx, searchReq)
		if err != nil {
			log.Fatalf("Failed to search in %s: %v", name, err)
		}
		if len(searchResp.Results) != 2 {
			log.Fatalf("Expected 2 search results in %s, got %d", name, len(searchResp.Results))
		}
		fmt.Printf("  ✅ Search in %s returned %d results\n", name, len(searchResp.Results))
	}

	// Test 9: Drop all remaining collections
	fmt.Println("\nTest 9: Dropping all remaining collections...")
	listResp3, err := client.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		log.Fatalf("Failed to list collections: %v", err)
	}
	for _, coll := range listResp3.Collections {
		dropReq := &pb.DropCollectionRequest{CollectionName: coll.CollectionName}
		_, err := client.DropCollection(ctx, dropReq)
		if err != nil {
			log.Fatalf("Failed to drop collection %s: %v", coll.CollectionName, err)
		}
		fmt.Printf("  ✅ Dropped %s\n", coll.CollectionName)
	}

	// Test 10: Verify all collections are gone
	fmt.Println("\nTest 10: Verifying all collections are gone...")
	listResp4, err := client.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		log.Fatalf("Failed to list collections: %v", err)
	}
	if len(listResp4.Collections) != 0 {
		log.Fatalf("Expected 0 collections, got %d: %v", len(listResp4.Collections), listResp4.Collections)
	}
	fmt.Printf("  ✅ All collections successfully dropped\n")

	// Test 11: Rapid create/drop cycle
	fmt.Println("\nTest 11: Rapid create/drop cycle (stress test)...")
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("stress_test_%d", i)
		createReq := &pb.CreateCollectionRequest{
			CollectionName: name,
			Dimension:      128,
			Metric:         pb.CreateCollectionRequest_L2,
			IndexType:      pb.CreateCollectionRequest_FLAT,
		}
		_, err := client.CreateCollection(ctx, createReq)
		if err != nil {
			log.Fatalf("Failed to create %s: %v", name, err)
		}

		dropReq := &pb.DropCollectionRequest{CollectionName: name}
		_, err = client.DropCollection(ctx, dropReq)
		if err != nil {
			log.Fatalf("Failed to drop %s: %v", name, err)
		}
	}
	fmt.Printf("  ✅ Completed 10 rapid create/drop cycles\n")

	// Final verification
	listRespFinal, err := client.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		log.Fatalf("Failed to list collections: %v", err)
	}
	if len(listRespFinal.Collections) != 0 {
		log.Fatalf("Expected 0 collections after stress test, got %d", len(listRespFinal.Collections))
	}

	fmt.Println("\n========================================")
	fmt.Println("✅ ALL COMPREHENSIVE TESTS PASSED!")
	fmt.Println("========================================")
	fmt.Println("\nTest Summary:")
	fmt.Println("  ✅ Multiple collection types created and dropped")
	fmt.Println("  ✅ Partial drops verified (remaining collections intact)")
	fmt.Println("  ✅ Non-existent collection drop handled correctly")
	fmt.Println("  ✅ Collection recreation with different params works")
	fmt.Println("  ✅ Vector insert/search works after drops")
	fmt.Println("  ✅ All collections can be dropped cleanly")
	fmt.Println("  ✅ Rapid create/drop cycles stable")
	fmt.Println("\n🎉 DropCollection is production-ready!")
}
