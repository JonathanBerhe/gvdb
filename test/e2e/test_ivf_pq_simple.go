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
	serverAddr := "localhost:50051"
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewVectorDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test 1: Create collection with IVF_PQ
	fmt.Println("Creating collection with IVF_PQ index...")
	createReq := &pb.CreateCollectionRequest{
		CollectionName: "test_ivf_pq",
		Dimension:      384,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_IVF_PQ,
	}

	createResp, err := client.CreateCollection(ctx, createReq)
	if err != nil {
		log.Fatalf("Failed to create IVF_PQ collection: %v", err)
	}
	fmt.Printf("✅ IVF_PQ collection created (ID: %d)\n", createResp.CollectionId)

	// Test 2: Drop collection
	fmt.Println("Dropping collection...")
	dropReq := &pb.DropCollectionRequest{
		CollectionName: "test_ivf_pq",
	}

	_, err = client.DropCollection(ctx, dropReq)
	if err != nil {
		log.Fatalf("Failed to drop collection: %v", err)
	}
	fmt.Println("✅ Collection dropped successfully")

	fmt.Println("\n✅ All tests passed!")
}
