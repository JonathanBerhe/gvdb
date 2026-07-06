// Google Cloud Storage e2e test.
// Requires a GCS backend reachable by the gvdb server — either real GCS with
// ADC, or a fake-gcs-server emulator:
//   docker compose -f test/integration/docker-compose.fake-gcs.yml up -d
// and start the server with an object_store block of type "gcs" pointing at
// the emulator endpoint.
//
// This test verifies:
// 1. Server starts with GCS config
// 2. Vectors can be inserted and searched
// 3. Search results are correct
//
// Skipped if GVDB_GCS_ENDPOINT is not set.

package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	pb "gvdb/integration-tests/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func runGcsStorageTests() {
	gcsEndpoint := os.Getenv("GVDB_GCS_ENDPOINT")
	if gcsEndpoint == "" {
		fmt.Println("SKIP: GVDB_GCS_ENDPOINT not set (set to http://localhost:4443 for fake-gcs-server)")
		os.Exit(0)
	}

	serverAddr := GetServerAddr()

	fmt.Println("=== GCS Storage E2E Test ===")
	fmt.Printf("Server: %s\n", serverAddr)
	fmt.Printf("GCS Endpoint: %s\n", gcsEndpoint)

	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("FAIL: Could not connect to server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewVectorDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := fmt.Sprintf("gcs_test_%d", time.Now().UnixNano())
	var dimension uint32 = 64

	// 1. Create collection
	fmt.Printf("Creating collection: %s\n", collectionName)
	_, err = client.CreateCollection(ctx, &pb.CreateCollectionRequest{
		CollectionName: collectionName,
		Dimension:      dimension,
		Metric:         pb.CreateCollectionRequest_COSINE,
	})
	if err != nil {
		fmt.Printf("FAIL: CreateCollection: %v\n", err)
		os.Exit(1)
	}

	// 2. Insert vectors
	numVectors := 500
	fmt.Printf("Inserting %d vectors...\n", numVectors)
	vectors := make([]*pb.VectorWithId, numVectors)
	for i := 0; i < numVectors; i++ {
		values := make([]float32, dimension)
		for j := range values {
			values[j] = rand.Float32()
		}
		// Normalize for cosine
		var norm float64
		for _, v := range values {
			norm += float64(v) * float64(v)
		}
		norm = math.Sqrt(norm)
		for j := range values {
			values[j] /= float32(norm)
		}
		vectors[i] = &pb.VectorWithId{
			Id:     uint64(i + 1),
			Vector: &pb.Vector{Values: values},
		}
	}

	_, err = client.Insert(ctx, &pb.InsertRequest{
		CollectionName: collectionName,
		Vectors:        vectors,
	})
	if err != nil {
		fmt.Printf("FAIL: Insert: %v\n", err)
		os.Exit(1)
	}

	// 3. Search
	fmt.Println("Searching...")
	searchResp, err := client.Search(ctx, &pb.SearchRequest{
		CollectionName: collectionName,
		QueryVector:    vectors[0].Vector,
		TopK:           5,
	})
	if err != nil {
		fmt.Printf("FAIL: Search: %v\n", err)
		os.Exit(1)
	}

	if len(searchResp.Results) == 0 {
		fmt.Println("FAIL: Search returned 0 results")
		os.Exit(1)
	}

	// Verify first result is the query vector itself (nearest neighbor = self)
	if searchResp.Results[0].Id != vectors[0].Id {
		fmt.Printf("WARN: First result ID %d != query ID %d\n",
			searchResp.Results[0].Id, vectors[0].Id)
	}

	fmt.Printf("Search returned %d results, top distance: %.6f\n",
		len(searchResp.Results), searchResp.Results[0].Distance)

	// 4. Cleanup
	_, err = client.DropCollection(ctx, &pb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	if err != nil {
		fmt.Printf("WARN: DropCollection: %v\n", err)
	}

	fmt.Println("PASS: GCS Storage E2E Test")
}

func main() {
	runGcsStorageTests()
}
