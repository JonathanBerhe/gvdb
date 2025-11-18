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
	serverAddr             = "localhost:50051"
	metadataTestCollection = "metadata_test_collection"
	metadataTestDimension  = 128
	metadataTestTimeout    = 30 * time.Second
)

type MetadataTest struct {
	conn   *grpc.ClientConn
	client pb.VectorDBServiceClient
}

func NewMetadataTest() (*MetadataTest, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	return &MetadataTest{
		conn:   conn,
		client: pb.NewVectorDBServiceClient(conn),
	}, nil
}

func (t *MetadataTest) Close() {
	if t.conn != nil {
		t.conn.Close()
	}
}

func (t *MetadataTest) createCollection() error {
	ctx, cancel := context.WithTimeout(context.Background(), metadataTestTimeout)
	defer cancel()

	// Try to drop collection if it exists (ignore errors)
	dropReq := &pb.DropCollectionRequest{
		CollectionName: metadataTestCollection,
	}
	t.client.DropCollection(ctx, dropReq)

	req := &pb.CreateCollectionRequest{
		CollectionName: metadataTestCollection,
		Dimension:      metadataTestDimension,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	}

	resp, err := t.client.CreateCollection(ctx, req)
	if err != nil {
		return fmt.Errorf("create collection failed: %v", err)
	}

	fmt.Printf("✅ Created collection '%s' (ID: %d): %s\n", metadataTestCollection, resp.CollectionId, resp.Message)
	return nil
}

func (t *MetadataTest) insertVectorsWithMetadata() error {
	ctx, cancel := context.WithTimeout(context.Background(), metadataTestTimeout)
	defer cancel()

	// Generate 100 vectors with varied metadata
	vectors := make([]*pb.VectorWithId, 100)

	brands := []string{"Nike", "Adidas", "Puma", "Reebok"}
	categories := []string{"Running", "Basketball", "Training", "Casual"}

	for i := 0; i < 100; i++ {
		vec := generateRandomVector(metadataTestDimension)

		// Create metadata for each vector
		metadata := &pb.Metadata{
			Fields: make(map[string]*pb.MetadataValue),
		}

		// Price: ranges from 50 to 250
		metadata.Fields["price"] = &pb.MetadataValue{
			Value: &pb.MetadataValue_IntValue{IntValue: int64(50 + i*2)},
		}

		// Brand: cycle through brands
		metadata.Fields["brand"] = &pb.MetadataValue{
			Value: &pb.MetadataValue_StringValue{StringValue: brands[i%len(brands)]},
		}

		// Category: cycle through categories
		metadata.Fields["category"] = &pb.MetadataValue{
			Value: &pb.MetadataValue_StringValue{StringValue: categories[i%len(categories)]},
		}

		// Rating: 3.0 to 5.0
		rating := 3.0 + (float64(i%20) * 0.1)
		metadata.Fields["rating"] = &pb.MetadataValue{
			Value: &pb.MetadataValue_DoubleValue{DoubleValue: rating},
		}

		// In stock: alternate true/false
		metadata.Fields["in_stock"] = &pb.MetadataValue{
			Value: &pb.MetadataValue_BoolValue{BoolValue: i%2 == 0},
		}

		vectors[i] = &pb.VectorWithId{
			Id:       uint64(i + 1),
			Vector:   vec,
			Metadata: metadata,
		}
	}

	req := &pb.InsertRequest{
		CollectionName: metadataTestCollection,
		Vectors:        vectors,
	}

	resp, err := t.client.Insert(ctx, req)
	if err != nil {
		return fmt.Errorf("insert failed: %v", err)
	}

	fmt.Printf("✅ Inserted %d vectors with metadata: %s\n", resp.InsertedCount, resp.Message)
	return nil
}

func (t *MetadataTest) searchWithSimpleFilter() error {
	ctx, cancel := context.WithTimeout(context.Background(), metadataTestTimeout)
	defer cancel()

	query := generateRandomVector(metadataTestDimension)

	// Search for Nike products under $150
	req := &pb.SearchRequest{
		CollectionName: metadataTestCollection,
		QueryVector:    query,
		TopK:           10,
		Filter:         "price < 150 AND brand = 'Nike'",
		ReturnMetadata: true,
	}

	resp, err := t.client.Search(ctx, req)
	if err != nil {
		return fmt.Errorf("search with filter failed: %v", err)
	}

	fmt.Printf("✅ Search with filter completed in %.2fms\n", resp.QueryTimeMs)
	fmt.Printf("   Filter: price < 150 AND brand = 'Nike'\n")
	fmt.Printf("   Found %d results:\n", len(resp.Results))

	// Verify all results match the filter
	for i, result := range resp.Results {
		if i >= 3 {
			break
		}

		// Verify metadata is returned
		if result.Metadata == nil {
			return fmt.Errorf("expected metadata in result, got nil")
		}

		priceVal := result.Metadata.Fields["price"]
		brandVal := result.Metadata.Fields["brand"]

		if priceVal == nil || brandVal == nil {
			return fmt.Errorf("missing price or brand in metadata")
		}

		price := priceVal.GetIntValue()
		brand := brandVal.GetStringValue()

		// Verify filter conditions
		if price >= 150 {
			return fmt.Errorf("result violates filter: price=%d >= 150", price)
		}
		if brand != "Nike" {
			return fmt.Errorf("result violates filter: brand=%s != 'Nike'", brand)
		}

		fmt.Printf("      %d. ID=%d, distance=%.4f, price=%d, brand=%s\n",
			i+1, result.Id, result.Distance, price, brand)
	}

	if len(resp.Results) == 0 {
		return fmt.Errorf("search returned no results, expected at least some Nike products under $150")
	}

	return nil
}

func (t *MetadataTest) searchWithComplexFilter() error {
	ctx, cancel := context.WithTimeout(context.Background(), metadataTestTimeout)
	defer cancel()

	query := generateRandomVector(metadataTestDimension)

	// Search for in-stock items that are either (Nike under $100) or (Adidas with rating > 4.0)
	req := &pb.SearchRequest{
		CollectionName: metadataTestCollection,
		QueryVector:    query,
		TopK:           10,
		Filter:         "in_stock = true AND ((brand = 'Nike' AND price < 100) OR (brand = 'Adidas' AND rating > 4.0))",
		ReturnMetadata: true,
	}

	resp, err := t.client.Search(ctx, req)
	if err != nil {
		return fmt.Errorf("search with complex filter failed: %v", err)
	}

	fmt.Printf("✅ Search with complex filter completed in %.2fms\n", resp.QueryTimeMs)
	fmt.Printf("   Filter: in_stock = true AND ((brand = 'Nike' AND price < 100) OR (brand = 'Adidas' AND rating > 4.0))\n")
	fmt.Printf("   Found %d results:\n", len(resp.Results))

	// Verify all results match the complex filter
	for i, result := range resp.Results {
		if i >= 3 {
			break
		}

		if result.Metadata == nil {
			return fmt.Errorf("expected metadata in result, got nil")
		}

		inStock := result.Metadata.Fields["in_stock"].GetBoolValue()
		brand := result.Metadata.Fields["brand"].GetStringValue()
		price := result.Metadata.Fields["price"].GetIntValue()
		rating := result.Metadata.Fields["rating"].GetDoubleValue()

		// Verify filter conditions
		if !inStock {
			return fmt.Errorf("result violates filter: in_stock=false")
		}

		nikeCondition := (brand == "Nike" && price < 100)
		adidasCondition := (brand == "Adidas" && rating > 4.0)

		if !nikeCondition && !adidasCondition {
			return fmt.Errorf("result violates filter: neither condition met (brand=%s, price=%d, rating=%.1f)",
				brand, price, rating)
		}

		fmt.Printf("      %d. ID=%d, distance=%.4f, brand=%s, price=%d, rating=%.1f, in_stock=%v\n",
			i+1, result.Id, result.Distance, brand, price, rating, inStock)
	}

	return nil
}

func (t *MetadataTest) searchWithLikeFilter() error {
	ctx, cancel := context.WithTimeout(context.Background(), metadataTestTimeout)
	defer cancel()

	query := generateRandomVector(metadataTestDimension)

	// Search for products with category containing "ing" (Running, Training)
	req := &pb.SearchRequest{
		CollectionName: metadataTestCollection,
		QueryVector:    query,
		TopK:           10,
		Filter:         "category LIKE '%ing'",
		ReturnMetadata: true,
	}

	resp, err := t.client.Search(ctx, req)
	if err != nil {
		return fmt.Errorf("search with LIKE filter failed: %v", err)
	}

	fmt.Printf("✅ Search with LIKE filter completed in %.2fms\n", resp.QueryTimeMs)
	fmt.Printf("   Filter: category LIKE '%%ing'\n")
	fmt.Printf("   Found %d results:\n", len(resp.Results))

	// Verify all results match the LIKE filter
	for i, result := range resp.Results {
		if i >= 3 {
			break
		}

		if result.Metadata == nil {
			return fmt.Errorf("expected metadata in result, got nil")
		}

		category := result.Metadata.Fields["category"].GetStringValue()

		// Verify LIKE condition (ends with "ing")
		if len(category) < 3 || category[len(category)-3:] != "ing" {
			return fmt.Errorf("result violates filter: category=%s doesn't end with 'ing'", category)
		}

		fmt.Printf("      %d. ID=%d, distance=%.4f, category=%s\n",
			i+1, result.Id, result.Distance, category)
	}

	if len(resp.Results) == 0 {
		return fmt.Errorf("search returned no results, expected categories ending with 'ing'")
	}

	return nil
}

func (t *MetadataTest) searchWithInFilter() error {
	ctx, cancel := context.WithTimeout(context.Background(), metadataTestTimeout)
	defer cancel()

	query := generateRandomVector(metadataTestDimension)

	// Search for specific brands using IN operator
	req := &pb.SearchRequest{
		CollectionName: metadataTestCollection,
		QueryVector:    query,
		TopK:           10,
		Filter:         "brand IN ('Nike', 'Adidas')",
		ReturnMetadata: true,
	}

	resp, err := t.client.Search(ctx, req)
	if err != nil {
		return fmt.Errorf("search with IN filter failed: %v", err)
	}

	fmt.Printf("✅ Search with IN filter completed in %.2fms\n", resp.QueryTimeMs)
	fmt.Printf("   Filter: brand IN ('Nike', 'Adidas')\n")
	fmt.Printf("   Found %d results:\n", len(resp.Results))

	// Verify all results match the IN filter
	for i, result := range resp.Results {
		if i >= 3 {
			break
		}

		if result.Metadata == nil {
			return fmt.Errorf("expected metadata in result, got nil")
		}

		brand := result.Metadata.Fields["brand"].GetStringValue()

		// Verify IN condition
		if brand != "Nike" && brand != "Adidas" {
			return fmt.Errorf("result violates filter: brand=%s not in ('Nike', 'Adidas')", brand)
		}

		fmt.Printf("      %d. ID=%d, distance=%.4f, brand=%s\n",
			i+1, result.Id, result.Distance, brand)
	}

	if len(resp.Results) == 0 {
		return fmt.Errorf("search returned no results, expected Nike or Adidas products")
	}

	return nil
}

func (t *MetadataTest) searchWithoutFilter() error {
	ctx, cancel := context.WithTimeout(context.Background(), metadataTestTimeout)
	defer cancel()

	query := generateRandomVector(metadataTestDimension)

	// Search without filter but request metadata
	req := &pb.SearchRequest{
		CollectionName: metadataTestCollection,
		QueryVector:    query,
		TopK:           10,
		Filter:         "", // No filter
		ReturnMetadata: true,
	}

	resp, err := t.client.Search(ctx, req)
	if err != nil {
		return fmt.Errorf("search without filter failed: %v", err)
	}

	fmt.Printf("✅ Search without filter (with metadata) completed in %.2fms\n", resp.QueryTimeMs)
	fmt.Printf("   Found %d results:\n", len(resp.Results))

	// Verify metadata is returned
	for i, result := range resp.Results {
		if i >= 3 {
			break
		}

		if result.Metadata == nil {
			return fmt.Errorf("expected metadata in result, got nil")
		}

		brand := result.Metadata.Fields["brand"].GetStringValue()
		price := result.Metadata.Fields["price"].GetIntValue()

		fmt.Printf("      %d. ID=%d, distance=%.4f, brand=%s, price=%d\n",
			i+1, result.Id, result.Distance, brand, price)
	}

	if len(resp.Results) == 0 {
		return fmt.Errorf("search returned no results")
	}

	return nil
}

func (t *MetadataTest) searchWithoutMetadata() error {
	ctx, cancel := context.WithTimeout(context.Background(), metadataTestTimeout)
	defer cancel()

	query := generateRandomVector(metadataTestDimension)

	// Search with filter but don't request metadata
	req := &pb.SearchRequest{
		CollectionName: metadataTestCollection,
		QueryVector:    query,
		TopK:           10,
		Filter:         "price < 100",
		ReturnMetadata: false, // Don't return metadata
	}

	resp, err := t.client.Search(ctx, req)
	if err != nil {
		return fmt.Errorf("search without metadata return failed: %v", err)
	}

	fmt.Printf("✅ Search with filter (without metadata return) completed in %.2fms\n", resp.QueryTimeMs)
	fmt.Printf("   Filter: price < 100\n")
	fmt.Printf("   Found %d results (metadata not requested):\n", len(resp.Results))

	for i, result := range resp.Results {
		if i >= 3 {
			break
		}
		fmt.Printf("      %d. ID=%d, distance=%.4f\n", i+1, result.Id, result.Distance)
	}

	if len(resp.Results) == 0 {
		return fmt.Errorf("search returned no results")
	}

	return nil
}

func (t *MetadataTest) dropCollection() error {
	ctx, cancel := context.WithTimeout(context.Background(), metadataTestTimeout)
	defer cancel()

	req := &pb.DropCollectionRequest{
		CollectionName: metadataTestCollection,
	}

	resp, err := t.client.DropCollection(ctx, req)
	if err != nil {
		return fmt.Errorf("drop collection failed: %v", err)
	}

	fmt.Printf("✅ Dropped collection '%s': %s\n", metadataTestCollection, resp.Message)
	return nil
}

func RunMetadataTest() bool {
	fmt.Println("======================================================================")
	fmt.Println("GVDB Metadata Filtering Test")
	fmt.Println("======================================================================")
	fmt.Println()

	test, err := NewMetadataTest()
	if err != nil {
		log.Printf("❌ Metadata Test FAILED: Cannot connect to server")
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
		{"Creating test collection", test.createCollection},
		{"Inserting vectors with metadata", test.insertVectorsWithMetadata},
		{"Search with simple filter (price < 150 AND brand = 'Nike')", test.searchWithSimpleFilter},
		{"Search with complex filter (nested AND/OR)", test.searchWithComplexFilter},
		{"Search with LIKE filter (category LIKE '%ing')", test.searchWithLikeFilter},
		{"Search with IN filter (brand IN ('Nike', 'Adidas'))", test.searchWithInFilter},
		{"Search without filter (with metadata return)", test.searchWithoutFilter},
		{"Search with filter (without metadata return)", test.searchWithoutMetadata},
		{"Cleaning up (dropping collection)", test.dropCollection},
	}

	for i, step := range steps {
		fmt.Printf("Step %d: %s...\n", i+1, step.name)
		if err := step.fn(); err != nil {
			log.Printf("❌ Metadata Test FAILED: %v", err)
			return false
		}
		fmt.Println()
	}

	fmt.Println("======================================================================")
	fmt.Println("✅ Metadata Filtering Test PASSED")
	fmt.Println("======================================================================")
	return true
}

func generateRandomVector(dim uint32) *pb.Vector {
	values := make([]float32, dim)
	var sum float32
	for i := range values {
		values[i] = rand.Float32()*2 - 1 // Range [-1, 1]
		sum += values[i] * values[i]
	}

	// Normalize
	norm := float32(1.0 / (sum + 1e-10))
	for i := range values {
		values[i] *= norm
	}

	return &pb.Vector{
		Values:    values,
		Dimension: dim,
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	success := RunMetadataTest()
	if !success {
		log.Fatal("Metadata test failed")
	}
}
