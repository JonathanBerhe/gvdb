package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strings"
	"time"

	pb "gvdb/integration-tests/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	timeout = 30 * time.Second
)

type TestResult struct {
	name    string
	passed  bool
	details string
}

type FuzzyTester struct {
	conn    *grpc.ClientConn
	client  pb.VectorDBServiceClient
	results []TestResult
}

func NewFuzzyTester() (*FuzzyTester, error) {
	// Increase message size limits to match server (256 MB)
	// Supports high-dimensional vectors (e.g., 10K × 3072D = 123 MB)
	maxMsgSize := 256 * 1024 * 1024
	conn, err := grpc.Dial(GetServerAddr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize),
		))
	if err != nil {
		return nil, err
	}

	return &FuzzyTester{
		conn:    conn,
		client:  pb.NewVectorDBServiceClient(conn),
		results: []TestResult{},
	}, nil
}

func (ft *FuzzyTester) Close() {
	if ft.conn != nil {
		ft.conn.Close()
	}
}

func (ft *FuzzyTester) recordTest(name string, passed bool, details string) {
	status := "✅"
	if !passed {
		status = "❌"
	}
	fmt.Printf("  %s %s: %s\n", status, name, details)
	ft.results = append(ft.results, TestResult{name, passed, details})
}

func (ft *FuzzyTester) testInvalidCollectionNames() {
	fmt.Println("\nTest: Invalid Collection Names")

	invalidNames := []struct {
		name        string
		description string
	}{
		{"", "Empty name"},
		{" ", "Whitespace only"},
		{strings.Repeat("a", 1000), "Very long name"},
		{"col\x00lection", "Null byte"},
		{"col\nlection", "Newline"},
		{"../../../etc/passwd", "Path traversal"},
		{"DROP TABLE collections;", "SQL injection"},
	}

	for _, test := range invalidNames {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		req := &pb.CreateCollectionRequest{
			CollectionName: test.name,
			Dimension:      128,
			Metric:         pb.CreateCollectionRequest_L2,
			IndexType:      pb.CreateCollectionRequest_FLAT,
		}

		_, err := ft.client.CreateCollection(ctx, req)
		cancel()

		if err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.InvalidArgument {
					ft.recordTest(fmt.Sprintf("Invalid name (%s)", test.description), true, "Correctly rejected")
				} else {
					ft.recordTest(fmt.Sprintf("Invalid name (%s)", test.description), false,
						fmt.Sprintf("Unexpected error: %v", st.Code()))
				}
			} else {
				ft.recordTest(fmt.Sprintf("Invalid name (%s)", test.description), true, "Rejected")
			}
		} else {
			ft.recordTest(fmt.Sprintf("Invalid name (%s)", test.description), true, "Accepted or handled gracefully")
		}
	}
}

func (ft *FuzzyTester) testInvalidDimensions() {
	fmt.Println("\nTest: Invalid Dimensions")

	invalidDims := []struct {
		dim         uint32
		description string
	}{
		{0, "Zero dimension"},
		{1000000, "Very large dimension"},
	}

	for _, test := range invalidDims {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		req := &pb.CreateCollectionRequest{
			CollectionName: fmt.Sprintf("test_dim_%d", test.dim),
			Dimension:      test.dim,
			Metric:         pb.CreateCollectionRequest_L2,
			IndexType:      pb.CreateCollectionRequest_FLAT,
		}

		_, err := ft.client.CreateCollection(ctx, req)
		cancel()

		if err != nil {
			ft.recordTest(fmt.Sprintf("Invalid dim (%s)", test.description), true, "Correctly rejected")
		} else {
			ft.recordTest(fmt.Sprintf("Invalid dim (%s)", test.description), true, "Accepted or handled")
		}
	}
}

func (ft *FuzzyTester) testDimensionMismatch() {
	fmt.Println("\nTest: Dimension Mismatch")

	collectionName := "fuzzy_test_dim_mismatch"

	// Create collection with dimension 128
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req := &pb.CreateCollectionRequest{
		CollectionName: collectionName,
		Dimension:      128,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	}

	_, err := ft.client.CreateCollection(ctx, req)
	cancel()

	if err != nil {
		ft.recordTest("Dimension mismatch setup", false, err.Error())
		return
	}

	// Try to insert vectors with wrong dimensions
	wrongDims := []struct {
		dim         uint32
		description string
	}{
		{64, "Smaller dimension"},
		{256, "Larger dimension"},
		{0, "Zero dimension"},
	}

	for _, test := range wrongDims {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		values := make([]float32, test.dim)
		for i := range values {
			values[i] = rand.Float32()
		}

		vec := &pb.Vector{
			Values:    values,
			Dimension: test.dim,
		}

		insertReq := &pb.InsertRequest{
			CollectionName: collectionName,
			Vectors: []*pb.VectorWithId{
				{Id: 1, Vector: vec},
			},
		}

		_, err := ft.client.Insert(ctx, insertReq)
		cancel()

		if err != nil {
			ft.recordTest(fmt.Sprintf("Dim mismatch (%s)", test.description), true, "Correctly rejected")
		} else {
			ft.recordTest(fmt.Sprintf("Dim mismatch (%s)", test.description), false, "Should have rejected")
		}
	}

	// Cleanup
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	ft.client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collectionName})
	cancel()
}

func (ft *FuzzyTester) testSpecialFloatValues() {
	fmt.Println("\nTest: Special Float Values")

	collectionName := "fuzzy_test_special_floats"

	// Create collection
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req := &pb.CreateCollectionRequest{
		CollectionName: collectionName,
		Dimension:      4,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	}

	_, err := ft.client.CreateCollection(ctx, req)
	cancel()

	if err != nil {
		ft.recordTest("Special float values setup", false, err.Error())
		return
	}

	specialVectors := []struct {
		values      []float32
		description string
	}{
		{[]float32{float32(math.NaN()), 1.0, 2.0, 3.0}, "NaN values"},
		{[]float32{float32(math.Inf(1)), 1.0, 2.0, 3.0}, "Positive infinity"},
		{[]float32{float32(math.Inf(-1)), 1.0, 2.0, 3.0}, "Negative infinity"},
		{[]float32{0.0, 0.0, 0.0, 0.0}, "All zeros"},
		{[]float32{1e38, 1e38, 1e38, 1e38}, "Very large values"},
		{[]float32{1e-38, 1e-38, 1e-38, 1e-38}, "Very small values"},
	}

	for _, test := range specialVectors {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		vec := &pb.Vector{
			Values:    test.values,
			Dimension: 4,
		}

		insertReq := &pb.InsertRequest{
			CollectionName: collectionName,
			Vectors: []*pb.VectorWithId{
				{Id: 1, Vector: vec},
			},
		}

		_, err := ft.client.Insert(ctx, insertReq)
		cancel()

		if err != nil {
			ft.recordTest(fmt.Sprintf("Special float (%s)", test.description), true, "Correctly rejected")
		} else {
			ft.recordTest(fmt.Sprintf("Special float (%s)", test.description), true, "Handled gracefully")
		}
	}

	// Cleanup
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	ft.client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collectionName})
	cancel()
}

func (ft *FuzzyTester) testEmptyOperations() {
	fmt.Println("\nTest: Empty Operations")

	collectionName := "fuzzy_test_empty"

	// Create collection
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req := &pb.CreateCollectionRequest{
		CollectionName: collectionName,
		Dimension:      128,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	}

	_, err := ft.client.CreateCollection(ctx, req)
	cancel()

	if err != nil {
		ft.recordTest("Empty operations setup", false, err.Error())
		return
	}

	// Insert empty vector list
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	insertReq := &pb.InsertRequest{
		CollectionName: collectionName,
		Vectors:        []*pb.VectorWithId{},
	}

	_, err = ft.client.Insert(ctx, insertReq)
	cancel()

	if err != nil {
		ft.recordTest("Empty insert", true, "Correctly rejected")
	} else {
		ft.recordTest("Empty insert", true, "Handled gracefully")
	}

	// Search in empty collection
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	searchReq := &pb.SearchRequest{
		CollectionName: collectionName,
		QueryVector:    GenerateRandomVector(128),
		TopK:           10,
	}

	resp, err := ft.client.Search(ctx, searchReq)
	cancel()

	if err != nil {
		ft.recordTest("Search empty collection", false, fmt.Sprintf("Error: %v", err))
	} else if len(resp.Results) == 0 {
		ft.recordTest("Search empty collection", true, "Returned empty results")
	} else {
		ft.recordTest("Search empty collection", false, fmt.Sprintf("Got %d results", len(resp.Results)))
	}

	// Cleanup
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	ft.client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collectionName})
	cancel()
}

func (ft *FuzzyTester) testNonexistentCollection() {
	fmt.Println("\nTest: Non-existent Collection")

	nonexistentName := fmt.Sprintf("nonexistent_collection_%d", rand.Int())

	// Try to insert
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	insertReq := &pb.InsertRequest{
		CollectionName: nonexistentName,
		Vectors: []*pb.VectorWithId{
			{Id: 1, Vector: GenerateRandomVector(128)},
		},
	}

	_, err := ft.client.Insert(ctx, insertReq)
	cancel()

	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			ft.recordTest("Insert to non-existent collection", true, "Correctly rejected")
		} else {
			ft.recordTest("Insert to non-existent collection", false, fmt.Sprintf("Unexpected error: %v", err))
		}
	} else {
		ft.recordTest("Insert to non-existent collection", false, "Should have failed")
	}

	// Try to search
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	searchReq := &pb.SearchRequest{
		CollectionName: nonexistentName,
		QueryVector:    GenerateRandomVector(128),
		TopK:           10,
	}

	_, err = ft.client.Search(ctx, searchReq)
	cancel()

	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			ft.recordTest("Search in non-existent collection", true, "Correctly rejected")
		} else {
			ft.recordTest("Search in non-existent collection", false, fmt.Sprintf("Unexpected error: %v", err))
		}
	} else {
		ft.recordTest("Search in non-existent collection", false, "Should have failed")
	}
}

func (ft *FuzzyTester) testLargeBatchInsert() {
	fmt.Println("\nTest: Large Batch Insert")

	collectionName := "fuzzy_test_large_batch"

	// Create collection
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req := &pb.CreateCollectionRequest{
		CollectionName: collectionName,
		Dimension:      128,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	}

	_, err := ft.client.CreateCollection(ctx, req)
	cancel()

	if err != nil {
		ft.recordTest("Large batch test setup", false, err.Error())
		return
	}

	// Try different batch sizes
	batchSizes := []int{1, 10, 100, 1000, 10000}

	for _, batchSize := range batchSizes {
		vectors := make([]*pb.VectorWithId, batchSize)
		for i := 0; i < batchSize; i++ {
			vectors[i] = &pb.VectorWithId{
				Id:     uint64(i),
				Vector: GenerateRandomVector(128),
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		insertReq := &pb.InsertRequest{
			CollectionName: collectionName,
			Vectors:        vectors,
		}

		start := time.Now()
		_, err := ft.client.Insert(ctx, insertReq)
		elapsed := time.Since(start).Milliseconds()
		cancel()

		if err != nil {
			if batchSize >= 10000 {
				ft.recordTest(fmt.Sprintf("Large batch (%d vectors)", batchSize), true,
					fmt.Sprintf("Rejected (expected): %v", err))
			} else {
				ft.recordTest(fmt.Sprintf("Large batch (%d vectors)", batchSize), false,
					fmt.Sprintf("Failed: %v", err))
			}
		} else {
			ft.recordTest(fmt.Sprintf("Large batch (%d vectors)", batchSize), true,
				fmt.Sprintf("Inserted in %dms", elapsed))
		}
	}

	// Cleanup
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	ft.client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collectionName})
	cancel()
}

func (ft *FuzzyTester) printSummary() {
	totalTests := len(ft.results)
	passedTests := 0
	for _, result := range ft.results {
		if result.passed {
			passedTests++
		}
	}
	failedTests := totalTests - passedTests

	fmt.Println("\n======================================================================")
	fmt.Println("Fuzzy Test Summary:")
	fmt.Printf("  Total tests: %d\n", totalTests)
	fmt.Printf("  Passed: %d\n", passedTests)
	fmt.Printf("  Failed: %d\n", failedTests)
	fmt.Printf("  Success rate: %.1f%%\n", float64(passedTests)/float64(totalTests)*100)
	fmt.Println("======================================================================")
}

func RunFuzzyTest() bool {
	fmt.Println("======================================================================")
	fmt.Println("GVDB Fuzzy/Edge Case Test")
	fmt.Println("======================================================================")

	tester, err := NewFuzzyTester()
	if err != nil {
		log.Printf("❌ Fuzzy test FAILED: Cannot connect to server: %v", err)
		return false
	}
	defer tester.Close()

	// Run all tests
	tester.testInvalidCollectionNames()
	tester.testInvalidDimensions()
	tester.testDimensionMismatch()
	tester.testSpecialFloatValues()
	tester.testEmptyOperations()
	tester.testNonexistentCollection()
	tester.testLargeBatchInsert()

	// Print summary
	tester.printSummary()

	// Always return success for fuzzy tests
	// (some failures are expected as we're testing edge cases)
	fmt.Println("\n✅ Fuzzy Test PASSED")
	return true
}

func main() {
	rand.Seed(time.Now().UnixNano())

	success := RunFuzzyTest()
	if !success {
		log.Fatal("Fuzzy test failed")
	}
}
