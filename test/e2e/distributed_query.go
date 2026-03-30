// test/e2e/distributed_query.go
// E2E test for distributed query execution with metadata sync and segment replication

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pb "gvdb/integration-tests/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func testDistributedQuery(coordinatorAddr, dataNodeAddr string) error {
	fmt.Println("\n=== Testing Distributed Query Execution ===")

	// Connect to coordinator
	coordConn, err := grpc.Dial(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %v", err)
	}
	defer coordConn.Close()
	coordClient := pb.NewVectorDBServiceClient(coordConn)

	// Connect to data node
	dataConn, err := grpc.Dial(dataNodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to data node: %v", err)
	}
	defer dataConn.Close()
	dataClient := pb.NewVectorDBServiceClient(dataConn)

	ctx := context.Background()

	// Step 1: Create collection on coordinator
	fmt.Println("\n1. Creating collection on coordinator...")
	collectionName := "distributed_test_collection"
	createReq := &pb.CreateCollectionRequest{
		CollectionName: collectionName,
		Dimension:      128,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	}

	createResp, err := coordClient.CreateCollection(ctx, createReq)
	if err != nil {
		return fmt.Errorf("failed to create collection: %v", err)
	}
	fmt.Printf("   ✓ Collection created with ID: %d\n", createResp.CollectionId)

	// Step 2: Insert vectors on coordinator
	fmt.Println("\n2. Inserting 100 vectors on coordinator...")
	var vectors []*pb.VectorWithId
	for i := 0; i < 100; i++ {
		values := make([]float32, 128)
		for j := 0; j < 128; j++ {
			values[j] = float32(i*128 + j)
		}
		vectors = append(vectors, &pb.VectorWithId{
			Id: uint64(i + 1),
			Vector: &pb.Vector{
				Values:    values,
				Dimension: 128,
			},
		})
	}

	insertReq := &pb.InsertRequest{
		CollectionName: collectionName,
		Vectors:        vectors,
	}

	insertResp, err := coordClient.Insert(ctx, insertReq)
	if err != nil {
		return fmt.Errorf("failed to insert vectors: %v", err)
	}
	fmt.Printf("   ✓ Inserted %d vectors\n", insertResp.InsertedCount)

	// Step 3: Wait a moment for any async operations
	time.Sleep(500 * time.Millisecond)

	// Step 4: Search from data node (should trigger metadata + segment replication)
	fmt.Println("\n3. Searching from data node (triggers replication)...")
	queryVector := make([]float32, 128)
	for j := 0; j < 128; j++ {
		queryVector[j] = float32(j) // Similar to vector 0
	}

	searchReq := &pb.SearchRequest{
		CollectionName: collectionName,
		QueryVector: &pb.Vector{
			Values:    queryVector,
			Dimension: 128,
		},
		TopK: 5,
	}

	searchResp, err := dataClient.Search(ctx, searchReq)
	if err != nil {
		return fmt.Errorf("failed to search from data node: %v", err)
	}

	if len(searchResp.Results) == 0 {
		return fmt.Errorf("expected search results, got none")
	}

	fmt.Printf("   ✓ Search successful! Found %d results\n", len(searchResp.Results))
	fmt.Println("   Top 5 results:")
	for i, result := range searchResp.Results {
		fmt.Printf("      %d. ID=%d, Distance=%.4f\n", i+1, result.Id, result.Distance)
	}

	// Step 5: Verify the closest vector is ID 1 (vector 0, which we queried for)
	if searchResp.Results[0].Id != 1 {
		return fmt.Errorf("expected closest vector ID=1, got ID=%d", searchResp.Results[0].Id)
	}
	fmt.Println("   ✓ Correct nearest neighbor found (ID=1)")

	// Step 6: Second search from data node (should use cached metadata + segment)
	fmt.Println("\n4. Second search from data node (uses cache)...")
	queryVector2 := make([]float32, 128)
	for j := 0; j < 128; j++ {
		queryVector2[j] = float32(50*128 + j) // Similar to vector 50
	}

	searchReq2 := &pb.SearchRequest{
		CollectionName: collectionName,
		QueryVector: &pb.Vector{
			Values:    queryVector2,
			Dimension: 128,
		},
		TopK: 3,
	}

	searchResp2, err := dataClient.Search(ctx, searchReq2)
	if err != nil {
		return fmt.Errorf("failed on second search: %v", err)
	}

	fmt.Printf("   ✓ Second search successful! Found %d results\n", len(searchResp2.Results))
	fmt.Println("   Top 3 results:")
	for i, result := range searchResp2.Results {
		fmt.Printf("      %d. ID=%d, Distance=%.4f\n", i+1, result.Id, result.Distance)
	}

	if searchResp2.Results[0].Id != 51 { // Vector 50 has ID 51
		return fmt.Errorf("expected closest vector ID=51, got ID=%d", searchResp2.Results[0].Id)
	}
	fmt.Println("   ✓ Correct nearest neighbor found (ID=51)")

	// Step 7: Search for non-existent collection (should fail gracefully)
	fmt.Println("\n5. Testing error handling (non-existent collection)...")
	searchReq3 := &pb.SearchRequest{
		CollectionName: "nonexistent_collection",
		QueryVector: &pb.Vector{
			Values:    queryVector,
			Dimension: 128,
		},
		TopK: 5,
	}

	_, err = dataClient.Search(ctx, searchReq3)
	if err == nil {
		return fmt.Errorf("expected error for non-existent collection, got none")
	}
	fmt.Printf("   ✓ Correctly returned error: %v\n", err)

	// Step 8: Cleanup
	fmt.Println("\n6. Cleaning up...")
	dropReq := &pb.DropCollectionRequest{
		CollectionName: collectionName,
	}

	_, err = coordClient.DropCollection(ctx, dropReq)
	if err != nil {
		return fmt.Errorf("failed to drop collection: %v", err)
	}
	fmt.Println("   ✓ Collection dropped")

	fmt.Println("\n=== ✅ All distributed query tests passed! ===")
	return nil
}

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: distributed_query_test <coordinator_address> <data_node_address>")
	}

	coordinatorAddr := os.Args[1]
	dataNodeAddr := os.Args[2]

	fmt.Printf("Coordinator: %s\n", coordinatorAddr)
	fmt.Printf("Data Node:   %s\n", dataNodeAddr)

	if err := testDistributedQuery(coordinatorAddr, dataNodeAddr); err != nil {
		log.Fatalf("❌ Test failed: %v", err)
	}

	fmt.Println("\n🎉 Distributed query E2E test completed successfully!")
}
