package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	pb "gvdb/integration-tests/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	collectionName  = "e2e_test_collection"
	dimension       = 128
	numVectors      = 1000
	timeout         = 30 * time.Second
)

type E2ETest struct {
	conn   *grpc.ClientConn
	client pb.VectorDBServiceClient
}

func NewE2ETest() (*E2ETest, error) {
	serverAddr := GetServerAddr()
	fmt.Printf("\nStep 1: Connecting to server...\n")
	fmt.Printf("   Server address: %s\n", serverAddr)
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	return &E2ETest{
		conn:   conn,
		client: pb.NewVectorDBServiceClient(conn),
	}, nil
}

func (t *E2ETest) Close() {
	if t.conn != nil {
		t.conn.Close()
	}
}

func (t *E2ETest) healthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := t.client.HealthCheck(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("health check failed: %v", err)
	}

	if resp.Status != pb.HealthCheckResponse_SERVING {
		return fmt.Errorf("server not serving: %s", resp.Message)
	}

	fmt.Printf("✅ Health check: SERVING - %s\n", resp.Message)
	return nil
}

func (t *E2ETest) cleanupStale() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Drop collection if it exists from a previous failed run
	t.client.DropCollection(ctx, &pb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	return nil
}

func (t *E2ETest) createCollection() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req := &pb.CreateCollectionRequest{
		CollectionName: collectionName,
		Dimension:      dimension,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	}

	resp, err := t.client.CreateCollection(ctx, req)
	if err != nil {
		return fmt.Errorf("create collection failed: %v", err)
	}

	fmt.Printf("✅ Created collection '%s' (ID: %d): %s\n", collectionName, resp.CollectionId, resp.Message)
	return nil
}

func (t *E2ETest) insertVectors() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Generate random vectors
	vectors := make([]*pb.VectorWithId, numVectors)
	for i := 0; i < numVectors; i++ {
		vec := GenerateRandomVector(dimension)
		vectors[i] = &pb.VectorWithId{
			Id:     uint64(i + 1),
			Vector: vec,
		}
	}

	req := &pb.InsertRequest{
		CollectionName: collectionName,
		Vectors:        vectors,
	}

	resp, err := t.client.Insert(ctx, req)
	if err != nil {
		return fmt.Errorf("insert failed: %v", err)
	}

	fmt.Printf("✅ Inserted %d vectors: %s\n", resp.InsertedCount, resp.Message)
	return nil
}

func (t *E2ETest) search() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	query := GenerateRandomVector(dimension)
	req := &pb.SearchRequest{
		CollectionName: collectionName,
		QueryVector:    query,
		TopK:           10,
	}

	resp, err := t.client.Search(ctx, req)
	if err != nil {
		return fmt.Errorf("search failed: %v", err)
	}

	fmt.Printf("✅ Search completed in %.2fms\n", resp.QueryTimeMs)
	fmt.Printf("   Found %d results:\n", len(resp.Results))
	for i, result := range resp.Results {
		if i >= 5 { // Show top 5
			break
		}
		fmt.Printf("      %d. ID=%d, distance=%.4f\n", i+1, result.Id, result.Distance)
	}

	if len(resp.Results) == 0 {
		return fmt.Errorf("search returned no results")
	}

	return nil
}

func (t *E2ETest) getStats() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := t.client.GetStats(ctx, &pb.GetStatsRequest{})
	if err != nil {
		return fmt.Errorf("get stats failed: %v", err)
	}

	fmt.Printf("✅ Stats:\n")
	fmt.Printf("   Total collections: %d\n", resp.TotalCollections)
	fmt.Printf("   Total vectors: %d\n", resp.TotalVectors)
	fmt.Printf("   Total queries: %d\n", resp.TotalQueries)
	fmt.Printf("   Avg query time: %.2fms\n", resp.AvgQueryTimeMs)

	return nil
}

func (t *E2ETest) listCollections() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := t.client.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		return fmt.Errorf("list collections failed: %v", err)
	}

	fmt.Printf("✅ Collections (%d):\n", len(resp.Collections))
	for _, coll := range resp.Collections {
		fmt.Printf("   - %s (ID: %d, dim: %d, vectors: %d)\n",
			coll.CollectionName, coll.CollectionId, coll.Dimension, coll.VectorCount)
	}

	// Verify our collection is in the list
	found := false
	for _, coll := range resp.Collections {
		if coll.CollectionName == collectionName {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("collection '%s' not found in list", collectionName)
	}

	return nil
}

func (t *E2ETest) testGet() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Get vectors by ID (IDs 1-5 + non-existent 9999)
	req := &pb.GetRequest{
		CollectionName: collectionName,
		Ids:            []uint64{1, 2, 3, 4, 5, 9999}, // 9999 doesn't exist
		ReturnMetadata: false,
	}

	resp, err := t.client.Get(ctx, req)
	if err != nil {
		return fmt.Errorf("get failed: %v", err)
	}

	fmt.Printf("✅ Get vectors by ID:\n")
	fmt.Printf("   Found: %d vectors\n", len(resp.Vectors))
	fmt.Printf("   Not found: %d IDs\n", len(resp.NotFoundIds))

	if len(resp.Vectors) != 5 {
		return fmt.Errorf("expected 5 vectors, got %d", len(resp.Vectors))
	}

	if len(resp.NotFoundIds) != 1 || resp.NotFoundIds[0] != 9999 {
		return fmt.Errorf("expected not_found_ids=[9999], got %v", resp.NotFoundIds)
	}

	for i, vec := range resp.Vectors {
		if i < 3 {
			fmt.Printf("      ID=%d, dimension=%d\n", vec.Id, vec.Vector.Dimension)
		}
	}

	return nil
}

func (t *E2ETest) testUpdateMetadata() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Update metadata for vector ID 1 (merge mode)
	req := &pb.UpdateMetadataRequest{
		CollectionName: collectionName,
		Id:             1,
		Metadata: &pb.Metadata{
			Fields: map[string]*pb.MetadataValue{
				"price":    {Value: &pb.MetadataValue_DoubleValue{DoubleValue: 99.99}},
				"in_stock": {Value: &pb.MetadataValue_BoolValue{BoolValue: true}},
				"brand":    {Value: &pb.MetadataValue_StringValue{StringValue: "Nike"}},
			},
		},
		Merge: true, // Merge mode
	}

	resp, err := t.client.UpdateMetadata(ctx, req)
	if err != nil {
		return fmt.Errorf("update metadata failed: %v", err)
	}

	if !resp.Updated {
		return fmt.Errorf("metadata update reported failure: %s", resp.Message)
	}

	fmt.Printf("✅ Updated metadata: %s\n", resp.Message)

	// Verify metadata was updated
	getReq := &pb.GetRequest{
		CollectionName: collectionName,
		Ids:            []uint64{1},
		ReturnMetadata: true,
	}

	getResp, err := t.client.Get(ctx, getReq)
	if err != nil {
		return fmt.Errorf("get after update failed: %v", err)
	}

	if len(getResp.Vectors) != 1 {
		return fmt.Errorf("expected 1 vector, got %d", len(getResp.Vectors))
	}

	metadata := getResp.Vectors[0].Metadata
	if metadata == nil || len(metadata.Fields) == 0 {
		return fmt.Errorf("metadata not returned")
	}

	fmt.Printf("   Verified metadata fields: %d\n", len(metadata.Fields))
	if price, ok := metadata.Fields["price"]; ok {
		fmt.Printf("      price=%.2f\n", price.GetDoubleValue())
	}
	if brand, ok := metadata.Fields["brand"]; ok {
		fmt.Printf("      brand=%s\n", brand.GetStringValue())
	}

	return nil
}

func (t *E2ETest) testDelete() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Delete vectors with IDs 10-20 (and one that doesn't exist)
	idsToDelete := make([]uint64, 0, 12)
	for i := 10; i <= 20; i++ {
		idsToDelete = append(idsToDelete, uint64(i))
	}
	idsToDelete = append(idsToDelete, 9999) // Doesn't exist

	req := &pb.DeleteRequest{
		CollectionName: collectionName,
		Ids:            idsToDelete,
	}

	resp, err := t.client.Delete(ctx, req)
	if err != nil {
		return fmt.Errorf("delete failed: %v", err)
	}

	fmt.Printf("✅ Delete vectors:\n")
	fmt.Printf("   Deleted: %d vectors\n", resp.DeletedCount)
	fmt.Printf("   Not found: %d IDs\n", len(resp.NotFoundIds))

	if resp.DeletedCount != 11 {
		return fmt.Errorf("expected 11 deleted, got %d", resp.DeletedCount)
	}

	if len(resp.NotFoundIds) != 1 || resp.NotFoundIds[0] != 9999 {
		return fmt.Errorf("expected not_found_ids=[9999], got %v", resp.NotFoundIds)
	}

	// Verify vectors were deleted by trying to get them
	getReq := &pb.GetRequest{
		CollectionName: collectionName,
		Ids:            []uint64{10, 15, 20},
		ReturnMetadata: false,
	}

	getResp, err := t.client.Get(ctx, getReq)
	if err != nil {
		return fmt.Errorf("get after delete failed: %v", err)
	}

	if len(getResp.Vectors) != 0 {
		return fmt.Errorf("expected 0 vectors after delete, got %d", len(getResp.Vectors))
	}

	if len(getResp.NotFoundIds) != 3 {
		return fmt.Errorf("expected 3 not found IDs after delete, got %d", len(getResp.NotFoundIds))
	}

	fmt.Printf("   Verified deletion successful\n")
	return nil
}

func (t *E2ETest) verifyCleanup() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := t.client.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		return fmt.Errorf("list collections failed: %v", err)
	}

	fmt.Printf("✅ Collections (%d):\n", len(resp.Collections))
	for _, coll := range resp.Collections {
		fmt.Printf("   - %s (ID: %d, dim: %d, vectors: %d)\n",
			coll.CollectionName, coll.CollectionId, coll.Dimension, coll.VectorCount)
	}

	// Verify our collection is NOT in the list (it was dropped)
	for _, coll := range resp.Collections {
		if coll.CollectionName == collectionName {
			return fmt.Errorf("collection '%s' still exists after drop", collectionName)
		}
	}

	fmt.Println("   (Confirmed collection was successfully dropped)")
	return nil
}

func (t *E2ETest) dropCollection() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req := &pb.DropCollectionRequest{
		CollectionName: collectionName,
	}

	resp, err := t.client.DropCollection(ctx, req)
	if err != nil {
		return fmt.Errorf("drop collection failed: %v", err)
	}

	fmt.Printf("✅ Dropped collection '%s': %s\n", collectionName, resp.Message)
	return nil
}


func RunE2ETest() bool {
	fmt.Println("======================================================================")
	fmt.Println("GVDB End-to-End Test")
	fmt.Println("======================================================================")
	fmt.Println()

	test, err := NewE2ETest()
	if err != nil {
		log.Printf("❌ E2E Test FAILED: Cannot connect to server")
		log.Printf("   Make sure the server is running: ./build/bin/gvdb-single-node")
		log.Printf("   Error: %v", err)
		return false
	}
	defer test.Close()

	// Run test steps
	steps := []struct {
		name string
		fn   func() error
	}{
		{"Connecting to server", func() error { return nil }}, // Already connected
		{"Health check", test.healthCheck},
		{"Cleaning up stale data", test.cleanupStale},
		{"Creating collection", test.createCollection},
		{"Inserting vectors", test.insertVectors},
		{"Searching for similar vectors", test.search},
		{"Getting database stats", test.getStats},
		{"Listing collections", test.listCollections},
		{"Testing Get by ID", test.testGet},
		{"Testing Update Metadata", test.testUpdateMetadata},
		{"Testing Delete", test.testDelete},
		{"Running multiple searches", func() error {
			searchTimes := []float32{}
			for i := 0; i < 5; i++ {
				query := GenerateRandomVector(dimension)
				req := &pb.SearchRequest{
					CollectionName: collectionName,
					QueryVector:    query,
					TopK:           10,
				}

				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				resp, err := test.client.Search(ctx, req)
				cancel()

				if err != nil {
					return fmt.Errorf("search %d failed: %v", i+1, err)
				}
				searchTimes = append(searchTimes, resp.QueryTimeMs)
			}

			var avgTime float32
			for _, t := range searchTimes {
				avgTime += t
			}
			avgTime /= float32(len(searchTimes))
			fmt.Printf("   ✅ Completed 5 searches, avg time: %.2fms\n", avgTime)
			return nil
		}},
		{"Cleaning up (dropping collection)", test.dropCollection},
		{"Verifying cleanup", test.verifyCleanup},
	}

	for i, step := range steps {
		fmt.Printf("Step %d: %s...\n", i+1, step.name)
		if err := step.fn(); err != nil {
			log.Printf("❌ E2E Test FAILED: %v", err)
			return false
		}
		fmt.Println()
	}

	fmt.Println("======================================================================")
	fmt.Println("✅ E2E Test PASSED")
	fmt.Println("======================================================================")
	return true
}

func main() {
	rand.Seed(time.Now().UnixNano())

	success := RunE2ETest()
	if !success {
		log.Fatal("E2E test failed")
	}
}
