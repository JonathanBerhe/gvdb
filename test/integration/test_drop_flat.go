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

	// Test: Create FLAT collection
	fmt.Println("Creating FLAT collection...")
	createReq := &pb.CreateCollectionRequest{
		CollectionName: "test_flat_drop",
		Dimension:      384,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	}

	createResp, err := client.CreateCollection(ctx, createReq)
	if err != nil {
		log.Fatalf("Failed to create FLAT collection: %v", err)
	}
	fmt.Printf("✅ FLAT collection created (ID: %d)\n", createResp.CollectionId)

	// Test: Drop FLAT collection
	fmt.Println("Dropping FLAT collection...")
	dropReq := &pb.DropCollectionRequest{
		CollectionName: "test_flat_drop",
	}

	_, err = client.DropCollection(ctx, dropReq)
	if err != nil {
		log.Fatalf("Failed to drop collection: %v", err)
	}
	fmt.Println("✅ Collection dropped successfully")

	fmt.Println("\n✅ All tests passed!")
}
