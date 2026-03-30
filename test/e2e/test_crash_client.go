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
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Step 1: Create FLAT collection
	fmt.Println("Step 1: Creating FLAT collection...")
	createFlatReq := &pb.CreateCollectionRequest{
		CollectionName: "test_flat",
		Dimension:      384,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	}

	createFlatResp, err := client.CreateCollection(ctx, createFlatReq)
	if err != nil {
		log.Fatalf("Failed to create FLAT collection: %v", err)
	}
	fmt.Printf("✅ FLAT collection created (ID: %d)\n", createFlatResp.CollectionId)

	// Step 2: Drop FLAT collection
	fmt.Println("Step 2: Dropping FLAT collection...")
	dropFlatReq := &pb.DropCollectionRequest{
		CollectionName: "test_flat",
	}

	_, err = client.DropCollection(ctx, dropFlatReq)
	if err != nil {
		log.Fatalf("Failed to drop FLAT collection: %v", err)
	}
	fmt.Println("✅ FLAT collection dropped")

	// Small delay
	time.Sleep(1 * time.Second)

	// Step 3: Create IVF_PQ collection
	fmt.Println("Step 3: Creating IVF_PQ collection...")
	createIVFPQReq := &pb.CreateCollectionRequest{
		CollectionName: "test_ivf_pq",
		Dimension:      384,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_IVF_PQ,
	}

	createIVFPQResp, err := client.CreateCollection(ctx, createIVFPQReq)
	if err != nil {
		log.Fatalf("Failed to create IVF_PQ collection: %v", err)
	}
	fmt.Printf("✅ IVF_PQ collection created (ID: %d)\n", createIVFPQResp.CollectionId)

	// Step 4: Drop IVF_PQ collection
	fmt.Println("Step 4: Dropping IVF_PQ collection...")
	dropIVFPQReq := &pb.DropCollectionRequest{
		CollectionName: "test_ivf_pq",
	}

	_, err = client.DropCollection(ctx, dropIVFPQReq)
	if err != nil {
		log.Fatalf("Failed to drop IVF_PQ collection: %v", err)
	}
	fmt.Println("✅ IVF_PQ collection dropped")

	fmt.Println("\n✅ All tests passed!")
}
