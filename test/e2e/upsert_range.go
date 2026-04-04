package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	pb "gvdb/integration-tests/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	upsertCollection = "e2e_upsert_range"
	upsertDimension  = 4
)

type UpsertRangeTest struct {
	conn   *grpc.ClientConn
	client pb.VectorDBServiceClient
}

func NewUpsertRangeTest() (*UpsertRangeTest, error) {
	serverAddr := GetServerAddr()
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}
	return &UpsertRangeTest{conn: conn, client: pb.NewVectorDBServiceClient(conn)}, nil
}

func (t *UpsertRangeTest) Close() { t.conn.Close() }

func (t *UpsertRangeTest) Run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Cleanup
	t.client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: upsertCollection})

	// Create collection
	fmt.Println("Step 1: Create collection")
	_, err := t.client.CreateCollection(ctx, &pb.CreateCollectionRequest{
		CollectionName: upsertCollection,
		Dimension:      upsertDimension,
		Metric:         pb.CreateCollectionRequest_COSINE,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	})
	if err != nil {
		return fmt.Errorf("create collection: %v", err)
	}

	// Insert initial vectors
	fmt.Println("Step 2: Insert initial vectors")
	vectors := []*pb.VectorWithId{
		{Id: 1, Vector: &pb.Vector{Values: []float32{0.1, 0.0, 0.0, 0.0}, Dimension: upsertDimension},
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"name": {Value: &pb.MetadataValue_StringValue{StringValue: "original_1"}},
			}}},
		{Id: 2, Vector: &pb.Vector{Values: []float32{0.5, 0.0, 0.0, 0.0}, Dimension: upsertDimension},
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"name": {Value: &pb.MetadataValue_StringValue{StringValue: "original_2"}},
			}}},
		{Id: 3, Vector: &pb.Vector{Values: []float32{1.0, 0.0, 0.0, 0.0}, Dimension: upsertDimension},
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"name": {Value: &pb.MetadataValue_StringValue{StringValue: "original_3"}},
			}}},
	}
	_, err = t.client.Insert(ctx, &pb.InsertRequest{
		CollectionName: upsertCollection,
		Vectors:        vectors,
	})
	if err != nil {
		return fmt.Errorf("insert: %v", err)
	}

	// Test Upsert: update ID=2, insert ID=4
	fmt.Println("Step 3: Upsert (update ID=2, insert ID=4)")
	upsertResp, err := t.client.Upsert(ctx, &pb.UpsertRequest{
		CollectionName: upsertCollection,
		Vectors: []*pb.VectorWithId{
			{Id: 2, Vector: &pb.Vector{Values: []float32{0.9, 0.0, 0.0, 0.0}, Dimension: upsertDimension},
				Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
					"name": {Value: &pb.MetadataValue_StringValue{StringValue: "updated_2"}},
				}}},
			{Id: 4, Vector: &pb.Vector{Values: []float32{2.0, 0.0, 0.0, 0.0}, Dimension: upsertDimension},
				Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
					"name": {Value: &pb.MetadataValue_StringValue{StringValue: "new_4"}},
				}}},
		},
	})
	if err != nil {
		return fmt.Errorf("upsert: %v", err)
	}
	if upsertResp.InsertedCount != 1 || upsertResp.UpdatedCount != 1 {
		return fmt.Errorf("expected 1 inserted + 1 updated, got %d + %d",
			upsertResp.InsertedCount, upsertResp.UpdatedCount)
	}
	fmt.Printf("   Upserted: %d inserted, %d updated\n",
		upsertResp.InsertedCount, upsertResp.UpdatedCount)

	// Verify upsert: get ID=2 and check metadata
	fmt.Println("Step 4: Verify upsert metadata")
	getResp, err := t.client.Get(ctx, &pb.GetRequest{
		CollectionName: upsertCollection,
		Ids:            []uint64{2},
		ReturnMetadata: true,
	})
	if err != nil {
		return fmt.Errorf("get after upsert: %v", err)
	}
	if len(getResp.Vectors) != 1 {
		return fmt.Errorf("expected 1 vector, got %d", len(getResp.Vectors))
	}
	nameField := getResp.Vectors[0].Metadata.Fields["name"]
	if nameField.GetStringValue() != "updated_2" {
		return fmt.Errorf("expected metadata 'updated_2', got '%s'", nameField.GetStringValue())
	}
	fmt.Println("   Metadata correctly updated to 'updated_2'")

	// Verify total count is 4
	listResp, err := t.client.ListVectors(ctx, &pb.ListVectorsRequest{
		CollectionName:  upsertCollection,
		Limit:           100,
		IncludeMetadata: false,
	})
	if err != nil {
		return fmt.Errorf("list vectors: %v", err)
	}
	if listResp.TotalCount != 4 {
		return fmt.Errorf("expected 4 total vectors, got %d", listResp.TotalCount)
	}
	fmt.Printf("   Total vectors: %d (correct)\n", listResp.TotalCount)

	// Test Range Search
	fmt.Println("Step 5: Range search")
	rangeResp, err := t.client.RangeSearch(ctx, &pb.RangeSearchRequest{
		CollectionName: upsertCollection,
		QueryVector:    &pb.Vector{Values: []float32{0.0, 0.0, 0.0, 0.0}, Dimension: upsertDimension},
		Radius:         0.5,
		ReturnMetadata: true,
		MaxResults:     10,
	})
	if err != nil {
		return fmt.Errorf("range search: %v", err)
	}
	fmt.Printf("   Range search (radius=0.5): %d results in %.2fms\n",
		len(rangeResp.Results), rangeResp.QueryTimeMs)

	// All results should have distance <= 0.5
	for _, r := range rangeResp.Results {
		if r.Distance > 0.5+1e-6 {
			return fmt.Errorf("result ID=%d has distance %.4f > radius 0.5", r.Id, r.Distance)
		}
	}

	// Test Range Search with filter
	fmt.Println("Step 6: Range search with filter")
	filteredResp, err := t.client.RangeSearch(ctx, &pb.RangeSearchRequest{
		CollectionName: upsertCollection,
		QueryVector:    &pb.Vector{Values: []float32{0.0, 0.0, 0.0, 0.0}, Dimension: upsertDimension},
		Radius:         100.0, // Large radius to include all
		Filter:         "name = 'updated_2'",
		ReturnMetadata: true,
		MaxResults:     10,
	})
	if err != nil {
		return fmt.Errorf("range search with filter: %v", err)
	}
	if len(filteredResp.Results) != 1 {
		return fmt.Errorf("expected 1 filtered result, got %d", len(filteredResp.Results))
	}
	if filteredResp.Results[0].Id != 2 {
		return fmt.Errorf("expected result ID=2, got %d", filteredResp.Results[0].Id)
	}
	fmt.Println("   Filtered range search: 1 result (ID=2, correct)")

	// Test Range Search with max_results limit
	fmt.Println("Step 7: Range search with max_results")
	limitResp, err := t.client.RangeSearch(ctx, &pb.RangeSearchRequest{
		CollectionName: upsertCollection,
		QueryVector:    &pb.Vector{Values: []float32{0.0, 0.0, 0.0, 0.0}, Dimension: upsertDimension},
		Radius:         float32(math.MaxFloat32),
		MaxResults:     2,
	})
	if err != nil {
		return fmt.Errorf("range search max_results: %v", err)
	}
	if len(limitResp.Results) > 2 {
		return fmt.Errorf("expected <= 2 results, got %d", len(limitResp.Results))
	}
	fmt.Printf("   Max results=2: got %d results (correct)\n", len(limitResp.Results))

	// Cleanup
	t.client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: upsertCollection})

	fmt.Println("\nAll upsert and range search tests passed!")
	return nil
}

func main() {
	test, err := NewUpsertRangeTest()
	if err != nil {
		log.Fatalf("Failed to create test: %v", err)
	}
	defer test.Close()

	if err := test.Run(); err != nil {
		log.Fatalf("TEST FAILED: %v", err)
	}
}
