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
	fmt.Println("GVDB TTL (Time-to-Live) E2E Test")
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

	collectionName := "ttl_e2e_test"

	// Cleanup
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
	time.Sleep(3 * time.Second)

	// Step 2: Insert 5 vectors with TTL=3 seconds
	fmt.Println("\nStep 2: Inserting 5 vectors with TTL=3s...")
	ttlVectors := make([]*pb.VectorWithId, 5)
	for i := 0; i < 5; i++ {
		ttlVectors[i] = &pb.VectorWithId{
			Id:         uint64(i + 1),
			Vector:     GenerateRandomVector(4),
			TtlSeconds: 3,
		}
	}
	resp, err := client.Insert(ctx, &pb.InsertRequest{
		CollectionName: collectionName,
		Vectors:        ttlVectors,
	})
	if err != nil {
		log.Fatalf("Insert TTL vectors failed: %v", err)
	}
	fmt.Printf("   Inserted %d vectors with TTL=3s\n", resp.InsertedCount)

	// Step 3: Immediately search — should find results
	fmt.Println("\nStep 3: Searching immediately (should find results)...")
	searchResp, err := client.Search(ctx, &pb.SearchRequest{
		CollectionName: collectionName,
		QueryVector:    GenerateRandomVector(4),
		TopK:           10,
	})
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("   Found %d results (expected 5)\n", len(searchResp.Results))
	if len(searchResp.Results) == 0 {
		log.Fatalf("Expected results, got 0")
	}

	// Step 4: Get by ID — should find
	fmt.Println("\nStep 4: Get by ID (should find)...")
	getResp, err := client.Get(ctx, &pb.GetRequest{
		CollectionName: collectionName,
		Ids:            []uint64{1, 2, 3},
	})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("   Found %d vectors, not_found %d\n",
		len(getResp.Vectors), len(getResp.NotFoundIds))
	if len(getResp.Vectors) == 0 {
		log.Fatalf("Expected vectors, got 0")
	}

	// Step 5: Wait for TTL expiry
	fmt.Println("\nStep 5: Waiting 4 seconds for TTL expiry...")
	time.Sleep(4 * time.Second)

	// Step 6: Search again — should return 0 results (all expired)
	fmt.Println("\nStep 6: Searching after expiry (should find 0)...")
	searchResp2, err := client.Search(ctx, &pb.SearchRequest{
		CollectionName: collectionName,
		QueryVector:    GenerateRandomVector(4),
		TopK:           10,
	})
	if err != nil {
		log.Fatalf("Search after expiry failed: %v", err)
	}
	fmt.Printf("   Found %d results (expected 0)\n", len(searchResp2.Results))
	if len(searchResp2.Results) != 0 {
		log.Fatalf("Expected 0 results after TTL expiry, got %d", len(searchResp2.Results))
	}

	// Step 7: Get by ID — should be not found
	fmt.Println("\nStep 7: Get by ID after expiry (should be not found)...")
	getResp2, err := client.Get(ctx, &pb.GetRequest{
		CollectionName: collectionName,
		Ids:            []uint64{1, 2, 3},
	})
	if err != nil {
		log.Fatalf("Get after expiry failed: %v", err)
	}
	fmt.Printf("   Found %d vectors, not_found %d\n",
		len(getResp2.Vectors), len(getResp2.NotFoundIds))
	if len(getResp2.Vectors) != 0 {
		log.Fatalf("Expected 0 found vectors after TTL, got %d", len(getResp2.Vectors))
	}

	// Step 8: Insert permanent vectors (TTL=0), verify they survive
	fmt.Println("\nStep 8: Inserting 5 permanent vectors (no TTL)...")
	permVectors := make([]*pb.VectorWithId, 5)
	for i := 0; i < 5; i++ {
		permVectors[i] = &pb.VectorWithId{
			Id:     uint64(100 + i),
			Vector: GenerateRandomVector(4),
			// No TtlSeconds set → defaults to 0 → permanent
		}
	}
	_, err = client.Insert(ctx, &pb.InsertRequest{
		CollectionName: collectionName,
		Vectors:        permVectors,
	})
	if err != nil {
		log.Fatalf("Insert permanent vectors failed: %v", err)
	}

	time.Sleep(4 * time.Second)

	fmt.Println("   Waiting 4s then searching...")
	searchResp3, err := client.Search(ctx, &pb.SearchRequest{
		CollectionName: collectionName,
		QueryVector:    GenerateRandomVector(4),
		TopK:           10,
	})
	if err != nil {
		log.Fatalf("Search permanent vectors failed: %v", err)
	}
	fmt.Printf("   Found %d permanent vectors (expected 5)\n", len(searchResp3.Results))
	if len(searchResp3.Results) == 0 {
		log.Fatalf("Permanent vectors should not expire")
	}

	// Cleanup
	fmt.Println("\nStep 9: Cleanup...")
	client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collectionName})
	fmt.Println("   Collection dropped")

	fmt.Println("\n======================================================================")
	fmt.Println("TTL E2E Test PASSED")
	fmt.Println("======================================================================")
}
