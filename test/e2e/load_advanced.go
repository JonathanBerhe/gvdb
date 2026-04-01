package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "gvdb/integration-tests/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	timeout                   = 30 * time.Second
	advancedLoadTimeout       = 60 * time.Second
	concurrentSearchThreads   = 10
	searchesPerThread         = 100
	testDimension             = 384 // BERT-mini dimension
	testVectorsPerCollection  = 10000
)

type IndexTestConfig struct {
	Name      string
	IndexType pb.CreateCollectionRequest_IndexType
	Dimension uint32
	NumVectors int
}

type FilterTestResults struct {
	filterType    string
	totalOps      int
	successfulOps int32
	failedOps     int32
	latencies     []float64
	duration      time.Duration
	mutex         sync.Mutex
}

func (r *FilterTestResults) AddLatency(ms float64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.latencies = append(r.latencies, ms)
}

func (r *FilterTestResults) PrintResults() {
	successful := int(atomic.LoadInt32(&r.successfulOps))
	failed := int(atomic.LoadInt32(&r.failedOps))
	successRate := float64(successful) / float64(r.totalOps) * 100

	r.mutex.Lock()
	latencies := make([]float64, len(r.latencies))
	copy(latencies, r.latencies)
	r.mutex.Unlock()

	fmt.Printf("  %s Filtered Search:\n", r.filterType)
	fmt.Printf("    Total: %d, Success: %d, Failed: %d (%.1f%%)\n", r.totalOps, successful, failed, successRate)
	fmt.Printf("    Throughput: %.2f ops/sec\n", float64(successful)/r.duration.Seconds())

	if len(latencies) > 0 {
		sort.Float64s(latencies)
		p50 := latencies[len(latencies)*50/100]
		p95 := latencies[len(latencies)*95/100]
		p99 := latencies[len(latencies)*99/100]

		var sum float64
		for _, lat := range latencies {
			sum += lat
		}
		avg := sum / float64(len(latencies))

		fmt.Printf("    Latency: avg=%.2fms, p50=%.2fms, p95=%.2fms, p99=%.2fms\n", avg, p50, p95, p99)
	}
}

func generateMetadata(id int) *pb.Metadata {
	brands := []string{"Nike", "Adidas", "Puma", "Reebok"}
	categories := []string{"Running", "Training", "Basketball", "Soccer"}

	return &pb.Metadata{
		Fields: map[string]*pb.MetadataValue{
			"brand": {
				Value: &pb.MetadataValue_StringValue{
					StringValue: brands[id%len(brands)],
				},
			},
			"price": {
				Value: &pb.MetadataValue_DoubleValue{
					DoubleValue: float64(50 + (id%15)*10),
				},
			},
			"category": {
				Value: &pb.MetadataValue_StringValue{
					StringValue: categories[id%len(categories)],
				},
			},
			"in_stock": {
				Value: &pb.MetadataValue_BoolValue{
					BoolValue: id%3 != 0,
				},
			},
			"rating": {
				Value: &pb.MetadataValue_DoubleValue{
					DoubleValue: 3.0 + float64(id%3),
				},
			},
		},
	}
}

// Test concurrent filtered searches
func filteredSearchWorker(threadID int, collectionName string, filterExpr string, filterType string, results *FilterTestResults, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		atomic.AddInt32(&results.failedOps, int32(searchesPerThread))
		return
	}
	defer conn.Close()

	client := pb.NewVectorDBServiceClient(conn)

	for i := 0; i < searchesPerThread; i++ {
		query := GenerateRandomVector(testDimension)
		req := &pb.SearchRequest{
			CollectionName:  collectionName,
			QueryVector:     query,
			TopK:            10,
			Filter:          filterExpr,
			ReturnMetadata:  true,
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()
		_, err := client.Search(ctx, req)
		latency := time.Since(start).Seconds() * 1000
		cancel()

		if err != nil {
			atomic.AddInt32(&results.failedOps, 1)
		} else {
			atomic.AddInt32(&results.successfulOps, 1)
			results.AddLatency(latency)
		}
	}
}

func testConcurrentFilteredSearch(collectionName string) error {
	fmt.Println("\n======================================================================")
	fmt.Println("Concurrent Filtered Search Test")
	fmt.Printf("  Collection: %s\n", collectionName)
	fmt.Printf("  Threads: %d\n", concurrentSearchThreads)
	fmt.Printf("  Searches per thread: %d\n", searchesPerThread)
	fmt.Printf("  Total searches: %d\n", concurrentSearchThreads*searchesPerThread)
	fmt.Println("======================================================================")

	// Different filter types to test
	filterTests := []struct {
		name   string
		filter string
	}{
		{"Simple", "price < 100"},
		{"Complex", "price < 150 AND brand = 'Nike'"},
		{"WithOR", "(brand = 'Nike' AND price < 100) OR (brand = 'Adidas' AND rating > 4.0)"},
		{"NoFilter", ""},
	}

	for _, test := range filterTests {
		results := &FilterTestResults{
			filterType: test.name,
			totalOps:   concurrentSearchThreads * searchesPerThread,
			latencies:  make([]float64, 0, concurrentSearchThreads*searchesPerThread),
		}

		var wg sync.WaitGroup
		start := time.Now()

		for i := 0; i < concurrentSearchThreads; i++ {
			wg.Add(1)
			go filteredSearchWorker(i, collectionName, test.filter, test.name, results, &wg)
		}

		wg.Wait()
		results.duration = time.Since(start)
		results.PrintResults()
	}

	fmt.Println("======================================================================")
	return nil
}

// Test index type performance
func testIndexTypePerformance(config IndexTestConfig) error {
	fmt.Println("\n======================================================================")
	fmt.Printf("Index Type Test: %s\n", config.Name)
	fmt.Printf("  Dimension: %d\n", config.Dimension)
	fmt.Printf("  Vectors: %d\n", config.NumVectors)
	fmt.Println("======================================================================")

	collectionName := fmt.Sprintf("test_%s_%d", config.Name, time.Now().Unix())

	// Create collection
	conn, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewVectorDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Create collection with specific index type
	createReq := &pb.CreateCollectionRequest{
		CollectionName: collectionName,
		Dimension:      config.Dimension,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      config.IndexType,
	}

	fmt.Printf("  Creating collection with %s index...\n", config.Name)
	createResp, err := client.CreateCollection(ctx, createReq)
	if err != nil {
		return fmt.Errorf("failed to create collection: %v", err)
	}
	fmt.Printf("  ✅ Collection created (ID: %d)\n", createResp.CollectionId)

	// Insert vectors with metadata
	fmt.Printf("  Inserting %d vectors...\n", config.NumVectors)
	insertStart := time.Now()

	batchSize := 100
	numBatches := (config.NumVectors + batchSize - 1) / batchSize

	for b := 0; b < numBatches; b++ {
		vectors := make([]*pb.VectorWithId, 0, batchSize)
		start := b * batchSize
		end := start + batchSize
		if end > config.NumVectors {
			end = config.NumVectors
		}

		for i := start; i < end; i++ {
			vectors = append(vectors, &pb.VectorWithId{
				Id:       uint64(i + 1),
				Vector:   GenerateRandomVector(config.Dimension),
				Metadata: generateMetadata(i),
			})
		}

		insertReq := &pb.InsertRequest{
			CollectionName: collectionName,
			Vectors:        vectors,
		}

		ctx2, cancel2 := context.WithTimeout(context.Background(), advancedLoadTimeout)
		_, err := client.Insert(ctx2, insertReq)
		cancel2()

		if err != nil {
			return fmt.Errorf("failed to insert batch %d: %v", b, err)
		}

		if (b+1)%10 == 0 || b == numBatches-1 {
			fmt.Printf("  Progress: %d/%d batches\n", b+1, numBatches)
		}
	}

	insertDuration := time.Since(insertStart)
	fmt.Printf("  ✅ Insert completed in %.2fs (%.0f vectors/sec)\n",
		insertDuration.Seconds(),
		float64(config.NumVectors)/insertDuration.Seconds())

	// Run search benchmark
	fmt.Printf("  Running search benchmark...\n")
	searchResults := &FilterTestResults{
		filterType: config.Name,
		totalOps:   concurrentSearchThreads * searchesPerThread,
		latencies:  make([]float64, 0, concurrentSearchThreads*searchesPerThread),
	}

	var wg sync.WaitGroup
	searchStart := time.Now()

	for i := 0; i < concurrentSearchThreads; i++ {
		wg.Add(1)
		go filteredSearchWorker(i, collectionName, "", config.Name, searchResults, &wg)
	}

	wg.Wait()
	searchResults.duration = time.Since(searchStart)

	fmt.Printf("  ✅ Search benchmark completed:\n")
	searchResults.PrintResults()

	// Cleanup
	fmt.Printf("  Cleaning up...\n")
	ctx3, cancel3 := context.WithTimeout(context.Background(), timeout)
	defer cancel3()
	dropReq := &pb.DropCollectionRequest{CollectionName: collectionName}
	client.DropCollection(ctx3, dropReq)
	fmt.Printf("  ✅ Collection dropped\n")

	fmt.Println("======================================================================")
	return nil
}

func RunAdvancedLoadTest() bool {
	rand.Seed(time.Now().UnixNano())

	fmt.Println("======================================================================")
	fmt.Println("GVDB Advanced Load Test")
	fmt.Println("======================================================================")

	// First, create a collection with metadata for filtered search testing
	filterTestCollection := fmt.Sprintf("filter_test_%d", time.Now().Unix())

	fmt.Println("\nStep 1: Creating collection for filtered search tests...")
	conn, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect: %v", err)
		return false
	}

	client := pb.NewVectorDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	createReq := &pb.CreateCollectionRequest{
		CollectionName: filterTestCollection,
		Dimension:      testDimension,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	}

	_, err = client.CreateCollection(ctx, createReq)
	if err != nil {
		log.Printf("Failed to create collection: %v", err)
		conn.Close()
		return false
	}
	fmt.Println("✅ Collection created")

	// Insert vectors with metadata
	fmt.Printf("Inserting %d vectors with metadata...\n", testVectorsPerCollection)
	batchSize := 100
	numBatches := (testVectorsPerCollection + batchSize - 1) / batchSize

	for b := 0; b < numBatches; b++ {
		vectors := make([]*pb.VectorWithId, 0, batchSize)
		start := b * batchSize
		end := start + batchSize
		if end > testVectorsPerCollection {
			end = testVectorsPerCollection
		}

		for i := start; i < end; i++ {
			vectors = append(vectors, &pb.VectorWithId{
				Id:       uint64(i + 1),
				Vector:   GenerateRandomVector(testDimension),
				Metadata: generateMetadata(i),
			})
		}

		insertReq := &pb.InsertRequest{
			CollectionName: filterTestCollection,
			Vectors:        vectors,
		}

		ctx2, cancel2 := context.WithTimeout(context.Background(), advancedLoadTimeout)
		_, err := client.Insert(ctx2, insertReq)
		cancel2()

		if err != nil {
			log.Printf("Failed to insert batch %d: %v", b, err)
			conn.Close()
			return false
		}
	}
	fmt.Println("✅ Vectors inserted")
	conn.Close()

	// Test 1: Concurrent Filtered Searches
	fmt.Println("\n\n" + strings.Repeat("=", 70))
	fmt.Println("TEST 1: Concurrent Filtered Search Performance")
	fmt.Println(strings.Repeat("=", 70))
	if err := testConcurrentFilteredSearch(filterTestCollection); err != nil {
		log.Printf("Filtered search test failed: %v", err)
		return false
	}
	fmt.Println("✅ Filtered search test completed")

	// Cleanup filter test collection
	conn2, _ := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if conn2 != nil {
		client2 := pb.NewVectorDBServiceClient(conn2)
		ctx3, cancel3 := context.WithTimeout(context.Background(), timeout)
		dropReq := &pb.DropCollectionRequest{CollectionName: filterTestCollection}
		client2.DropCollection(ctx3, dropReq)
		cancel3()
		conn2.Close()
	}

	// Test 2: IVF_PQ Index Performance
	fmt.Println("\n\n" + strings.Repeat("=", 70))
	fmt.Println("TEST 2: IVF_PQ Index Performance")
	fmt.Println(strings.Repeat("=", 70))
	ivfPQConfig := IndexTestConfig{
		Name:       "IVF_PQ",
		IndexType:  pb.CreateCollectionRequest_IVF_PQ,
		Dimension:  testDimension,
		NumVectors: 5000,
	}
	if err := testIndexTypePerformance(ivfPQConfig); err != nil {
		log.Printf("IVF_PQ test failed: %v", err)
		return false
	}
	fmt.Println("✅ IVF_PQ test completed")

	// Test 3: IVF_SQ Index Performance
	fmt.Println("\n\n" + strings.Repeat("=", 70))
	fmt.Println("TEST 3: IVF_SQ Index Performance")
	fmt.Println(strings.Repeat("=", 70))
	ivfSQConfig := IndexTestConfig{
		Name:       "IVF_SQ",
		IndexType:  pb.CreateCollectionRequest_IVF_SQ,
		Dimension:  testDimension,
		NumVectors: 5000,
	}
	if err := testIndexTypePerformance(ivfSQConfig); err != nil {
		log.Printf("IVF_SQ test failed: %v", err)
		return false
	}
	fmt.Println("✅ IVF_SQ test completed")

	// Test 4: Compare FLAT vs HNSW vs IVF_FLAT
	fmt.Println("\n\n" + strings.Repeat("=", 70))
	fmt.Println("TEST 4: Index Type Comparison (FLAT vs HNSW vs IVF_FLAT)")
	fmt.Println(strings.Repeat("=", 70))

	configs := []IndexTestConfig{
		{
			Name:       "FLAT",
			IndexType:  pb.CreateCollectionRequest_FLAT,
			Dimension:  256,
			NumVectors: 3000,
		},
		{
			Name:       "HNSW",
			IndexType:  pb.CreateCollectionRequest_HNSW,
			Dimension:  256,
			NumVectors: 3000,
		},
		{
			Name:       "IVF_FLAT",
			IndexType:  pb.CreateCollectionRequest_IVF_FLAT,
			Dimension:  256,
			NumVectors: 3000,
		},
	}

	for _, config := range configs {
		if err := testIndexTypePerformance(config); err != nil {
			log.Printf("%s test failed: %v", config.Name, err)
			return false
		}
		time.Sleep(1 * time.Second) // Brief pause between tests
	}
	fmt.Println("✅ Index comparison completed")

	fmt.Println("\n\n" + strings.Repeat("=", 70))
	fmt.Println("✅ ALL ADVANCED LOAD TESTS PASSED! 🎉")
	fmt.Println(strings.Repeat("=", 70))

	return true
}

func main() {
	rand.Seed(time.Now().UnixNano())

	success := RunAdvancedLoadTest()
	if !success {
		log.Fatal("Advanced load test failed")
	}
}
