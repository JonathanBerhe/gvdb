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

const (
	ivfTQCollection = "e2e_ivf_turboquant"
	ivfTQDimension  = 768
)

type IVFTurboQuantTest struct {
	conn   *grpc.ClientConn
	client pb.VectorDBServiceClient
}

func NewIVFTurboQuantTest() (*IVFTurboQuantTest, error) {
	serverAddr := GetServerAddr()
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}
	return &IVFTurboQuantTest{conn: conn, client: pb.NewVectorDBServiceClient(conn)}, nil
}

func (t *IVFTurboQuantTest) Close() { t.conn.Close() }

func (t *IVFTurboQuantTest) Run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Cleanup
	t.client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: ivfTQCollection})

	// Step 1: Create collection with IVF_TURBOQUANT
	fmt.Println("Step 1: Create IVF_TURBOQUANT collection")
	_, err := t.client.CreateCollection(ctx, &pb.CreateCollectionRequest{
		CollectionName: ivfTQCollection,
		Dimension:      ivfTQDimension,
		Metric:         pb.CreateCollectionRequest_COSINE,
		IndexType:      pb.CreateCollectionRequest_IVF_TURBOQUANT,
	})
	if err != nil {
		return fmt.Errorf("create collection: %v", err)
	}

	// Step 2: Insert vectors with metadata
	fmt.Println("Step 2: Insert 200 vectors with metadata")
	vectors := make([]*pb.VectorWithId, 200)
	for i := 0; i < 200; i++ {
		vec := GenerateRandomVector(ivfTQDimension)
		vectors[i] = &pb.VectorWithId{
			Id:     uint64(i + 1),
			Vector: vec,
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"label": {Value: &pb.MetadataValue_StringValue{StringValue: fmt.Sprintf("vec_%d", i+1)}},
			}},
		}
	}

	_, err = t.client.Insert(ctx, &pb.InsertRequest{
		CollectionName: ivfTQCollection,
		Vectors:        vectors,
	})
	if err != nil {
		return fmt.Errorf("insert: %v", err)
	}

	// Step 3: Search
	fmt.Println("Step 3: Search for similar vectors")
	searchResp, err := t.client.Search(ctx, &pb.SearchRequest{
		CollectionName: ivfTQCollection,
		QueryVector:    vectors[0].Vector,
		TopK:           5,
		ReturnMetadata: true,
	})
	if err != nil {
		return fmt.Errorf("search: %v", err)
	}
	if len(searchResp.Results) == 0 {
		return fmt.Errorf("expected search results, got 0")
	}
	fmt.Printf("   Found %d results in %.2fms\n", len(searchResp.Results), searchResp.QueryTimeMs)

	// Verify first result has metadata
	if searchResp.Results[0].Metadata == nil || len(searchResp.Results[0].Metadata.Fields) == 0 {
		return fmt.Errorf("expected metadata in results")
	}
	fmt.Printf("   First result: ID=%d, distance=%.6f, label=%s\n",
		searchResp.Results[0].Id,
		searchResp.Results[0].Distance,
		searchResp.Results[0].Metadata.Fields["label"].GetStringValue())

	// Step 4: Verify collection in list
	fmt.Println("Step 4: Verify collection exists")
	listResp, err := t.client.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		return fmt.Errorf("list collections: %v", err)
	}
	found := false
	for _, c := range listResp.Collections {
		if c.CollectionName == ivfTQCollection {
			found = true
			fmt.Printf("   Collection: %s, vectors: %d\n", c.CollectionName, c.VectorCount)
		}
	}
	if !found {
		return fmt.Errorf("collection %s not found in list", ivfTQCollection)
	}

	// Step 5: Stress test — bulk insert + concurrent search
	fmt.Println("Step 5: Stress test (5000 vectors, 50 concurrent searches)")
	batchSize := 500
	totalVectors := 5000
	for batch := 0; batch < totalVectors/batchSize; batch++ {
		batchVecs := make([]*pb.VectorWithId, batchSize)
		for i := 0; i < batchSize; i++ {
			id := uint64(1000 + batch*batchSize + i)
			batchVecs[i] = &pb.VectorWithId{
				Id:     id,
				Vector: GenerateRandomVector(ivfTQDimension),
				Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
					"batch": {Value: &pb.MetadataValue_IntValue{IntValue: int64(batch)}},
				}},
			}
		}
		_, err = t.client.Insert(ctx, &pb.InsertRequest{
			CollectionName: ivfTQCollection,
			Vectors:        batchVecs,
		})
		if err != nil {
			return fmt.Errorf("stress insert batch %d: %v", batch, err)
		}
	}
	fmt.Printf("   Inserted %d vectors in %d batches\n", totalVectors, totalVectors/batchSize)

	// Concurrent searches
	numSearches := 50
	errCh := make(chan error, numSearches)
	start := time.Now()
	for i := 0; i < numSearches; i++ {
		go func() {
			_, err := t.client.Search(ctx, &pb.SearchRequest{
				CollectionName: ivfTQCollection,
				QueryVector:    GenerateRandomVector(ivfTQDimension),
				TopK:           10,
				ReturnMetadata: true,
			})
			errCh <- err
		}()
	}

	var searchErrors int
	for i := 0; i < numSearches; i++ {
		if err := <-errCh; err != nil {
			searchErrors++
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("   %d concurrent searches in %v (%d errors)\n",
		numSearches, elapsed, searchErrors)
	if searchErrors > 0 {
		return fmt.Errorf("%d search errors during stress test", searchErrors)
	}

	// Cleanup
	t.client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: ivfTQCollection})

	fmt.Println("\nAll IVF_TURBOQUANT tests passed!")
	return nil
}

func main() {
	test, err := NewIVFTurboQuantTest()
	if err != nil {
		log.Fatalf("Failed to create test: %v", err)
	}
	defer test.Close()

	if err := test.Run(); err != nil {
		log.Fatalf("TEST FAILED: %v", err)
	}
}
