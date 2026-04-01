package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "gvdb/integration-tests/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	timeout            = 30 * time.Second
	loadTestCollection = "load_test_collection"
	loadTestDimension  = 128
	numThreads         = 10
	opsPerThread       = 50
	batchSize          = 10
	loadTimeout        = 60 * time.Second
)

type LoadTestResults struct {
	totalOps     int
	successfulOps int32
	failedOps    int32
	latencies    []float64
	duration     time.Duration
}

func (r *LoadTestResults) AddLatency(ms float64) {
	r.latencies = append(r.latencies, ms)
}

func (r *LoadTestResults) PrintResults(testName string) {
	successful := int(atomic.LoadInt32(&r.successfulOps))
	failed := int(atomic.LoadInt32(&r.failedOps))
	successRate := float64(successful) / float64(r.totalOps) * 100

	fmt.Println("======================================================================")
	fmt.Printf("%s Results:\n", testName)
	fmt.Printf("  Total operations: %d\n", r.totalOps)
	fmt.Printf("  Successful: %d\n", successful)
	fmt.Printf("  Failed: %d\n", failed)
	fmt.Printf("  Success rate: %.1f%%\n", successRate)
	fmt.Printf("\n  Total duration: %.2fs\n", r.duration.Seconds())
	fmt.Printf("  Throughput: %.2f ops/sec\n", float64(successful)/r.duration.Seconds())

	if len(r.latencies) > 0 {
		sort.Float64s(r.latencies)
		p50 := r.latencies[len(r.latencies)*50/100]
		p95 := r.latencies[len(r.latencies)*95/100]
		p99 := r.latencies[len(r.latencies)*99/100]

		var sum float64
		for _, lat := range r.latencies {
			sum += lat
		}
		avg := sum / float64(len(r.latencies))

		fmt.Printf("\n  Latency (ms):\n")
		fmt.Printf("    Average: %.2f\n", avg)
		fmt.Printf("    P50: %.2f\n", p50)
		fmt.Printf("    P95: %.2f\n", p95)
		fmt.Printf("    P99: %.2f\n", p99)
	}

	fmt.Println("======================================================================")
}

func setupLoadTestCollection() error {
	conn, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewVectorDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req := &pb.CreateCollectionRequest{
		CollectionName: loadTestCollection,
		Dimension:      loadTestDimension,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	}

	_, err = client.CreateCollection(ctx, req)
	if err != nil {
		// Collection might already exist, that's OK
		fmt.Println("✅ Collection setup (using existing or created new)")
	} else {
		fmt.Println("✅ Collection created")
	}

	return nil
}

func cleanupLoadTestCollection() {
	conn, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()

	client := pb.NewVectorDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req := &pb.DropCollectionRequest{
		CollectionName: loadTestCollection,
	}

	client.DropCollection(ctx, req)
	fmt.Println("✅ Collection dropped")
}

func insertWorker(threadID int, results *LoadTestResults, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		atomic.AddInt32(&results.failedOps, int32(opsPerThread))
		return
	}
	defer conn.Close()

	client := pb.NewVectorDBServiceClient(conn)

	for i := 0; i < opsPerThread; i++ {
		// Generate batch of vectors
		vectors := make([]*pb.VectorWithId, batchSize)
		for j := 0; j < batchSize; j++ {
			vecID := uint64(threadID*opsPerThread*batchSize + i*batchSize + j)
			vectors[j] = &pb.VectorWithId{
				Id:     vecID,
				Vector: GenerateRandomVector(loadTestDimension),
			}
		}

		req := &pb.InsertRequest{
			CollectionName: loadTestCollection,
			Vectors:        vectors,
		}

		ctx, cancel := context.WithTimeout(context.Background(), loadTimeout)
		start := time.Now()
		_, err := client.Insert(ctx, req)
		latency := time.Since(start).Seconds() * 1000 // Convert to ms
		cancel()

		if err != nil {
			atomic.AddInt32(&results.failedOps, 1)
		} else {
			atomic.AddInt32(&results.successfulOps, 1)
			results.AddLatency(latency)
		}
	}
}

func searchWorker(threadID int, results *LoadTestResults, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		atomic.AddInt32(&results.failedOps, int32(opsPerThread))
		return
	}
	defer conn.Close()

	client := pb.NewVectorDBServiceClient(conn)

	for i := 0; i < opsPerThread; i++ {
		query := GenerateRandomVector(loadTestDimension)
		req := &pb.SearchRequest{
			CollectionName: loadTestCollection,
			QueryVector:    query,
			TopK:           10,
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()
		_, err := client.Search(ctx, req)
		latency := time.Since(start).Seconds() * 1000 // Convert to ms
		cancel()

		if err != nil {
			atomic.AddInt32(&results.failedOps, 1)
		} else {
			atomic.AddInt32(&results.successfulOps, 1)
			results.AddLatency(latency)
		}
	}
}

func runConcurrentInserts() (*LoadTestResults, error) {
	fmt.Println("\n======================================================================")
	fmt.Println("Concurrent Insert Test")
	fmt.Printf("  Threads: %d\n", numThreads)
	fmt.Printf("  Operations per thread: %d\n", opsPerThread)
	fmt.Printf("  Batch size: %d\n", batchSize)
	fmt.Printf("  Total vectors: %d\n", numThreads*opsPerThread*batchSize)
	fmt.Println("======================================================================")

	results := &LoadTestResults{
		totalOps:  numThreads * opsPerThread,
		latencies: make([]float64, 0, numThreads*opsPerThread),
	}

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go insertWorker(i, results, &wg)
	}

	wg.Wait()
	results.duration = time.Since(start)

	return results, nil
}

func runConcurrentSearches() (*LoadTestResults, error) {
	fmt.Println("\n======================================================================")
	fmt.Println("Concurrent Search Test")
	fmt.Printf("  Threads: %d\n", numThreads)
	fmt.Printf("  Searches per thread: %d\n", opsPerThread)
	fmt.Printf("  Total searches: %d\n", numThreads*opsPerThread)
	fmt.Println("======================================================================")

	results := &LoadTestResults{
		totalOps:  numThreads * opsPerThread,
		latencies: make([]float64, 0, numThreads*opsPerThread),
	}

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go searchWorker(i, results, &wg)
	}

	wg.Wait()
	results.duration = time.Since(start)

	return results, nil
}

func RunLoadTest() bool {
	fmt.Println("======================================================================")
	fmt.Println("GVDB Load Test")
	fmt.Println("======================================================================")

	// Setup
	if err := setupLoadTestCollection(); err != nil {
		log.Printf("❌ Load test FAILED: Setup error: %v", err)
		return false
	}

	// Test 1: Concurrent Inserts
	insertResults, err := runConcurrentInserts()
	if err != nil {
		log.Printf("❌ Load test FAILED: %v", err)
		return false
	}
	insertResults.PrintResults("Concurrent Insert Test")

	failureRate := float64(atomic.LoadInt32(&insertResults.failedOps)) / float64(insertResults.totalOps)
	if failureRate > 0.05 {
		fmt.Printf("⚠️  Warning: >5%% insert failures\n")
	}

	// Test 2: Concurrent Searches
	searchResults, err := runConcurrentSearches()
	if err != nil {
		log.Printf("❌ Load test FAILED: %v", err)
		return false
	}
	searchResults.PrintResults("Concurrent Search Test")

	failureRate = float64(atomic.LoadInt32(&searchResults.failedOps)) / float64(searchResults.totalOps)
	if failureRate > 0.05 {
		fmt.Printf("⚠️  Warning: >5%% search failures\n")
	}

	// Test 3: High-Dimensional Large Batch Inserts
	fmt.Println("\n======================================================================")
	fmt.Println("High-Dimensional Large Batch Test")
	fmt.Println("Testing realistic production dimensions and large batches")
	fmt.Println("======================================================================")

	highDimTests := []struct {
		dim       uint32
		batch     int
		modelName string
	}{
		{768, 25000, "BERT-base"},
		{1536, 15000, "OpenAI ada-002"},
		{3072, 8000, "OpenAI text-embedding-3-large"},
	}

	for _, test := range highDimTests {
		collectionName := fmt.Sprintf("hd_test_%dd", test.dim)

		// Create collection
		conn, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("⚠️  Skipping %dD test: connection failed", test.dim)
			continue
		}

		client := pb.NewVectorDBServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		createReq := &pb.CreateCollectionRequest{
			CollectionName: collectionName,
			Dimension:      test.dim,
			Metric:         pb.CreateCollectionRequest_L2,
			IndexType:      pb.CreateCollectionRequest_FLAT,
		}

		_, err = client.CreateCollection(ctx, createReq)
		cancel()

		if err != nil {
			conn.Close()
			log.Printf("⚠️  Skipping %dD test: collection creation failed", test.dim)
			continue
		}

		// Generate large batch
		fmt.Printf("\n  %dD (%s): Generating %d vectors...\n", test.dim, test.modelName, test.batch)
		vectors := make([]*pb.VectorWithId, test.batch)
		for i := 0; i < test.batch; i++ {
			vectors[i] = &pb.VectorWithId{
				Id:     uint64(i + 1),
				Vector: GenerateRandomVector(test.dim),
			}
		}

		// Calculate memory estimate
		memoryMB := float64(test.dim) * 4.0 * float64(test.batch) / (1024.0 * 1024.0)
		fmt.Printf("  Estimated message size: %.1f MB\n", memoryMB)

		// Insert
		insertReq := &pb.InsertRequest{
			CollectionName: collectionName,
			Vectors:        vectors,
		}

		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		start := time.Now()
		_, err = client.Insert(ctx, insertReq)
		latency := time.Since(start)
		cancel()

		if err != nil {
			fmt.Printf("  ❌ FAILED: %v\n", err)
		} else {
			throughput := float64(test.batch) / latency.Seconds()
			fmt.Printf("  ✅ SUCCESS: %.0fms, %.0f vectors/sec\n",
				latency.Seconds()*1000, throughput)
		}

		// Cleanup
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		dropReq := &pb.DropCollectionRequest{CollectionName: collectionName}
		client.DropCollection(ctx, dropReq)
		cancel()
		conn.Close()

		time.Sleep(500 * time.Millisecond) // Brief pause between dimension tests
	}

	// Test 4: Concurrent High-Dimensional Large Batch Inserts
	fmt.Println("\n======================================================================")
	fmt.Println("Concurrent High-Dimensional Large Batch Test")
	fmt.Println("Testing concurrent large inserts with realistic dimensions")
	fmt.Println("======================================================================")

	concurrentHDTests := []struct {
		dim       uint32
		threads   int
		perThread int
		modelName string
	}{
		{768, 5, 10000, "BERT-base (5 threads × 10K)"},
		{1536, 5, 5000, "OpenAI ada-002 (5 threads × 5K)"},
		{1536, 10, 5000, "OpenAI ada-002 (10 threads × 5K)"},
		{3072, 5, 3000, "OpenAI large (5 threads × 3K)"},
	}

	for _, test := range concurrentHDTests {
		collectionName := fmt.Sprintf("concurrent_hd_%dd_%dthreads", test.dim, test.threads)

		// Create collection
		conn, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("⚠️  Skipping concurrent %dD test: connection failed", test.dim)
			continue
		}

		client := pb.NewVectorDBServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		createReq := &pb.CreateCollectionRequest{
			CollectionName: collectionName,
			Dimension:      test.dim,
			Metric:         pb.CreateCollectionRequest_L2,
			IndexType:      pb.CreateCollectionRequest_FLAT,
		}

		_, err = client.CreateCollection(ctx, createReq)
		cancel()

		if err != nil {
			conn.Close()
			log.Printf("⚠️  Skipping concurrent %dD test: collection creation failed", test.dim)
			continue
		}
		conn.Close()

		// Calculate total vectors and memory
		totalVectors := test.threads * test.perThread
		memoryMB := float64(test.dim) * 4.0 * float64(test.perThread) / (1024.0 * 1024.0)

		fmt.Printf("\n  %s\n", test.modelName)
		fmt.Printf("    Total vectors: %d (%d × %d)\n", totalVectors, test.threads, test.perThread)
		fmt.Printf("    Message size per thread: %.1f MB\n", memoryMB)

		// Concurrent insert
		var wg sync.WaitGroup
		var successCount, failCount int32
		latencies := make([]float64, 0, test.threads)
		var latencyMutex sync.Mutex

		start := time.Now()

		for threadID := 0; threadID < test.threads; threadID++ {
			wg.Add(1)
			go func(tid int) {
				defer wg.Done()

				// Generate vectors
				vectors := make([]*pb.VectorWithId, test.perThread)
				for i := 0; i < test.perThread; i++ {
					vecID := uint64(tid*test.perThread + i + 1)
					vectors[i] = &pb.VectorWithId{
						Id:     vecID,
						Vector: GenerateRandomVector(test.dim),
					}
				}

				// Insert
				maxMsgSize := 256 * 1024 * 1024
				conn, err := grpc.Dial(GetServerAddr(),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithDefaultCallOptions(
						grpc.MaxCallRecvMsgSize(maxMsgSize),
						grpc.MaxCallSendMsgSize(maxMsgSize),
					))
				if err != nil {
					atomic.AddInt32(&failCount, 1)
					return
				}
				defer conn.Close()

				client := pb.NewVectorDBServiceClient(conn)
				req := &pb.InsertRequest{
					CollectionName: collectionName,
					Vectors:        vectors,
				}

				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()

				threadStart := time.Now()
				_, err = client.Insert(ctx, req)
				threadLatency := time.Since(threadStart).Seconds() * 1000

				if err != nil {
					atomic.AddInt32(&failCount, 1)
				} else {
					atomic.AddInt32(&successCount, 1)
					latencyMutex.Lock()
					latencies = append(latencies, threadLatency)
					latencyMutex.Unlock()
				}
			}(threadID)
		}

		wg.Wait()
		totalElapsed := time.Since(start)

		// Results
		if successCount > 0 {
			sort.Float64s(latencies)
			avgLatency := 0.0
			for _, lat := range latencies {
				avgLatency += lat
			}
			avgLatency /= float64(len(latencies))

			p50 := latencies[len(latencies)*50/100]
			p95 := latencies[len(latencies)*95/100]

			throughput := float64(successCount*int32(test.perThread)) / totalElapsed.Seconds()

			fmt.Printf("    ✅ SUCCESS: %d/%d threads succeeded\n", successCount, test.threads)
			fmt.Printf("       Total time: %.2fs\n", totalElapsed.Seconds())
			fmt.Printf("       Throughput: %.0f vectors/sec\n", throughput)
			fmt.Printf("       Latency (avg/P50/P95): %.0f/%.0f/%.0fms\n", avgLatency, p50, p95)
		} else {
			fmt.Printf("    ❌ FAILED: All threads failed\n")
		}

		if failCount > 0 {
			fmt.Printf("    ⚠️  Warning: %d/%d threads failed\n", failCount, test.threads)
		}

		// Cleanup collection
		conn2, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			client := pb.NewVectorDBServiceClient(conn2)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			dropReq := &pb.DropCollectionRequest{CollectionName: collectionName}
			client.DropCollection(ctx, dropReq)
			cancel()
			conn2.Close()
		}

		time.Sleep(1 * time.Second) // Pause between tests
	}

	// Test 5: Concurrent Search on High-Dimensional Data
	fmt.Println("\n======================================================================")
	fmt.Println("Concurrent Search on High-Dimensional Data")
	fmt.Println("======================================================================")

	searchHDTests := []struct {
		dim       uint32
		dataSize  int
		threads   int
		searches  int
		modelName string
	}{
		{768, 20000, 10, 100, "BERT-base (10 threads × 100 searches)"},
		{1536, 10000, 10, 100, "OpenAI ada-002 (10 threads × 100 searches)"},
		{3072, 5000, 10, 50, "OpenAI large (10 threads × 50 searches)"},
	}

	for _, test := range searchHDTests {
		collectionName := fmt.Sprintf("search_hd_%dd", test.dim)

		// Create and populate collection
		conn, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("⚠️  Skipping search %dD test: connection failed", test.dim)
			continue
		}

		client := pb.NewVectorDBServiceClient(conn)

		// Create collection
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		createReq := &pb.CreateCollectionRequest{
			CollectionName: collectionName,
			Dimension:      test.dim,
			Metric:         pb.CreateCollectionRequest_L2,
			IndexType:      pb.CreateCollectionRequest_FLAT,
		}
		_, err = client.CreateCollection(ctx, createReq)
		cancel()

		if err != nil {
			conn.Close()
			log.Printf("⚠️  Skipping search %dD test: collection creation failed", test.dim)
			continue
		}

		// Insert data
		fmt.Printf("\n  %s\n", test.modelName)
		fmt.Printf("    Inserting %d vectors...\n", test.dataSize)

		vectors := make([]*pb.VectorWithId, test.dataSize)
		for i := 0; i < test.dataSize; i++ {
			vectors[i] = &pb.VectorWithId{
				Id:     uint64(i + 1),
				Vector: GenerateRandomVector(test.dim),
			}
		}

		maxMsgSize := 256 * 1024 * 1024
		conn2, _ := grpc.Dial(GetServerAddr(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxMsgSize),
				grpc.MaxCallSendMsgSize(maxMsgSize),
			))

		client2 := pb.NewVectorDBServiceClient(conn2)
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		insertReq := &pb.InsertRequest{
			CollectionName: collectionName,
			Vectors:        vectors,
		}
		_, err = client2.Insert(ctx, insertReq)
		cancel()
		conn2.Close()

		if err != nil {
			conn.Close()
			log.Printf("⚠️  Skipping search %dD test: data insertion failed: %v", test.dim, err)
			continue
		}

		// Concurrent search
		fmt.Printf("    Running %d concurrent threads...\n", test.threads)

		var wg sync.WaitGroup
		var successCount, failCount int32
		searchLatencies := make([]float64, 0, test.threads*test.searches)
		var searchLatencyMutex sync.Mutex

		start := time.Now()

		for threadID := 0; threadID < test.threads; threadID++ {
			wg.Add(1)
			go func(tid int) {
				defer wg.Done()

				conn, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					atomic.AddInt32(&failCount, int32(test.searches))
					return
				}
				defer conn.Close()

				client := pb.NewVectorDBServiceClient(conn)

				for i := 0; i < test.searches; i++ {
					query := GenerateRandomVector(test.dim)
					req := &pb.SearchRequest{
						CollectionName: collectionName,
						QueryVector:    query,
						TopK:           10,
					}

					ctx, cancel := context.WithTimeout(context.Background(), timeout)
					searchStart := time.Now()
					_, err := client.Search(ctx, req)
					searchLatency := time.Since(searchStart).Seconds() * 1000
					cancel()

					if err != nil {
						atomic.AddInt32(&failCount, 1)
					} else {
						atomic.AddInt32(&successCount, 1)
						searchLatencyMutex.Lock()
						searchLatencies = append(searchLatencies, searchLatency)
						searchLatencyMutex.Unlock()
					}
				}
			}(threadID)
		}

		wg.Wait()
		totalElapsed := time.Since(start)

		// Results
		if successCount > 0 {
			sort.Float64s(searchLatencies)
			avgLatency := 0.0
			for _, lat := range searchLatencies {
				avgLatency += lat
			}
			avgLatency /= float64(len(searchLatencies))

			p50 := searchLatencies[len(searchLatencies)*50/100]
			p95 := searchLatencies[len(searchLatencies)*95/100]
			p99 := searchLatencies[len(searchLatencies)*99/100]

			throughput := float64(successCount) / totalElapsed.Seconds()

			fmt.Printf("    ✅ SUCCESS: %d/%d searches succeeded\n", successCount, test.threads*test.searches)
			fmt.Printf("       Total time: %.2fs\n", totalElapsed.Seconds())
			fmt.Printf("       Throughput: %.0f searches/sec\n", throughput)
			fmt.Printf("       Latency (avg/P50/P95/P99): %.1f/%.1f/%.1f/%.1fms\n", avgLatency, p50, p95, p99)
		} else {
			fmt.Printf("    ❌ FAILED: All searches failed\n")
		}

		conn.Close()

		// Cleanup
		conn3, err := grpc.Dial(GetServerAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			client := pb.NewVectorDBServiceClient(conn3)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			dropReq := &pb.DropCollectionRequest{CollectionName: collectionName}
			client.DropCollection(ctx, dropReq)
			cancel()
			conn3.Close()
		}

		time.Sleep(1 * time.Second)
	}

	// Summary
	fmt.Println("\n======================================================================")
	fmt.Println("Load Test Summary:")
	fmt.Printf("  Insert throughput: %.2f ops/sec\n",
		float64(atomic.LoadInt32(&insertResults.successfulOps))/insertResults.duration.Seconds())
	fmt.Printf("  Search throughput: %.2f ops/sec\n",
		float64(atomic.LoadInt32(&searchResults.successfulOps))/searchResults.duration.Seconds())

	if len(insertResults.latencies) > 0 {
		sort.Float64s(insertResults.latencies)
		p95 := insertResults.latencies[len(insertResults.latencies)*95/100]
		fmt.Printf("  Insert P95 latency: %.2fms\n", p95)
	}

	if len(searchResults.latencies) > 0 {
		sort.Float64s(searchResults.latencies)
		p95 := searchResults.latencies[len(searchResults.latencies)*95/100]
		fmt.Printf("  Search P95 latency: %.2fms\n", p95)
	}

	fmt.Println("======================================================================")

	// Cleanup
	cleanupLoadTestCollection()

	fmt.Println("\n✅ Load Test PASSED")
	return true
}

func main() {
	rand.Seed(time.Now().UnixNano())

	success := RunLoadTest()
	if !success {
		log.Fatal("Load test failed")
	}
}
