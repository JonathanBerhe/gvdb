package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	pb "gvdb/integration-tests/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func dialWithKey(addr, key string) (*grpc.ClientConn, pb.VectorDBServiceClient) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if key != "" {
		opts = append(opts, grpc.WithUnaryInterceptor(func(
			ctx context.Context,
			method string,
			req, reply interface{},
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			callOpts ...grpc.CallOption,
		) error {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}))
	}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	return conn, pb.NewVectorDBServiceClient(conn)
}

// expectOK fatals if the error is not nil.
func expectOK(label string, err error) {
	if err != nil {
		log.Fatalf("   FAIL: %s: %v", label, err)
	}
	fmt.Printf("   OK: %s\n", label)
}

// expectCode fatals if the error code doesn't match.
func expectCode(label string, err error, want codes.Code) {
	if err == nil {
		log.Fatalf("   FAIL: %s: expected %s, got success", label, want)
	}
	st, _ := status.FromError(err)
	got := st.Code()
	// gRPC interceptor cancellation can show as Canceled instead of Unauthenticated
	if want == codes.Unauthenticated && got == codes.Canceled {
		got = codes.Unauthenticated
	}
	if got != want {
		log.Fatalf("   FAIL: %s: expected %s, got %s (%s)", label, want, got, st.Message())
	}
	fmt.Printf("   OK: %s → %s\n", label, want)
}

func makeVector(id uint64) *pb.VectorWithId {
	return &pb.VectorWithId{
		Id:     id,
		Vector: GenerateRandomVector(4),
	}
}

func makeVectors(count int) []*pb.VectorWithId {
	vecs := make([]*pb.VectorWithId, count)
	for i := 0; i < count; i++ {
		vecs[i] = makeVector(uint64(i + 1))
	}
	return vecs
}

func main() {
	fmt.Println("======================================================================")
	fmt.Println("GVDB RBAC E2E Test")
	fmt.Println("======================================================================")

	// Config with 4 roles: admin, readwrite (scoped), readonly (wildcard), collection_admin (scoped)
	configContent := `
server:
  grpc_port: 50052
  auth:
    enabled: true
    api_keys: ["legacy-key"]
    roles:
      - key: "admin-key"
        role: admin
      - key: "rw-key"
        role: readwrite
        collections: ["rbac_test", "second_coll"]
      - key: "ro-key"
        role: readonly
        collections: ["*"]
      - key: "ca-key"
        role: collection_admin
        collections: ["rbac_test"]
storage:
  data_dir: /tmp/gvdb-rbac-test
consensus:
  node_id: 1
  single_node_mode: true
logging:
  level: info
index:
  default_index_type: flat
`
	tmpConfig := "/tmp/gvdb-rbac-test-config.yaml"
	os.WriteFile(tmpConfig, []byte(configContent), 0644)
	defer os.Remove(tmpConfig)

	serverBin := os.Getenv("GVDB_SERVER_BIN")
	if serverBin == "" {
		serverBin = "../../build/bin/gvdb-single-node"
	}

	os.RemoveAll("/tmp/gvdb-rbac-test")
	os.MkdirAll("/tmp/gvdb-rbac-test", 0755)
	cmd := exec.Command(serverBin, "--config", tmpConfig, "--port", "50052", "--data-dir", "/tmp/gvdb-rbac-test")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.RemoveAll("/tmp/gvdb-rbac-test")
	}()

	fmt.Println("\nWaiting for RBAC server to start...")
	time.Sleep(3 * time.Second)

	addr := "localhost:50052"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	connNoAuth, clientNoAuth := dialWithKey(addr, "")
	defer connNoAuth.Close()
	connAdmin, clientAdmin := dialWithKey(addr, "admin-key")
	defer connAdmin.Close()
	connLegacy, clientLegacy := dialWithKey(addr, "legacy-key")
	defer connLegacy.Close()
	connRw, clientRw := dialWithKey(addr, "rw-key")
	defer connRw.Close()
	connRo, clientRo := dialWithKey(addr, "ro-key")
	defer connRo.Close()
	connCa, clientCa := dialWithKey(addr, "ca-key")
	defer connCa.Close()

	passed := 0
	total := 0
	check := func(label string, f func()) {
		total++
		f()
		passed++
	}

	vectors := makeVectors(10)
	qv := GenerateRandomVector(4)

	// ====================================================================
	fmt.Println("\n--- Authentication: public endpoints skip auth ---")
	// ====================================================================

	check("HealthCheck without auth", func() {
		_, err := clientNoAuth.HealthCheck(ctx, &pb.HealthCheckRequest{})
		expectOK("HealthCheck", err)
	})

	check("GetStats without auth", func() {
		_, err := clientNoAuth.GetStats(ctx, &pb.GetStatsRequest{})
		expectOK("GetStats", err)
	})

	// ====================================================================
	fmt.Println("\n--- Authentication: rejection ---")
	// ====================================================================

	check("No key → UNAUTHENTICATED on ListCollections", func() {
		_, err := clientNoAuth.ListCollections(ctx, &pb.ListCollectionsRequest{})
		expectCode("ListCollections no auth", err, codes.Unauthenticated)
	})

	check("No key → UNAUTHENTICATED on Insert", func() {
		_, err := clientNoAuth.Insert(ctx, &pb.InsertRequest{
			CollectionName: "rbac_test", Vectors: vectors[:1],
		})
		expectCode("Insert no auth", err, codes.Unauthenticated)
	})

	check("Wrong key → UNAUTHENTICATED", func() {
		connBad, clientBad := dialWithKey(addr, "nonexistent-key")
		defer connBad.Close()
		_, err := clientBad.ListCollections(ctx, &pb.ListCollectionsRequest{})
		expectCode("ListCollections bad key", err, codes.Unauthenticated)
	})

	// ====================================================================
	fmt.Println("\n--- Admin role: full access ---")
	// ====================================================================

	check("Admin CreateCollection", func() {
		_, err := clientAdmin.CreateCollection(ctx, &pb.CreateCollectionRequest{
			CollectionName: "rbac_test", Dimension: 4,
			Metric: pb.CreateCollectionRequest_L2, IndexType: pb.CreateCollectionRequest_FLAT,
		})
		expectOK("CreateCollection rbac_test", err)
	})

	check("Admin CreateCollection (other_coll)", func() {
		_, err := clientAdmin.CreateCollection(ctx, &pb.CreateCollectionRequest{
			CollectionName: "other_coll", Dimension: 4,
			Metric: pb.CreateCollectionRequest_L2, IndexType: pb.CreateCollectionRequest_FLAT,
		})
		expectOK("CreateCollection other_coll", err)
	})

	check("Admin Insert", func() {
		_, err := clientAdmin.Insert(ctx, &pb.InsertRequest{
			CollectionName: "rbac_test", Vectors: vectors,
		})
		expectOK("Insert 10 vectors", err)
	})

	check("Admin Search", func() {
		resp, err := clientAdmin.Search(ctx, &pb.SearchRequest{
			CollectionName: "rbac_test", QueryVector: qv, TopK: 5,
		})
		expectOK("Search", err)
		fmt.Printf("   Found %d results\n", len(resp.Results))
	})

	check("Admin Get", func() {
		_, err := clientAdmin.Get(ctx, &pb.GetRequest{
			CollectionName: "rbac_test", Ids: []uint64{1, 2},
		})
		expectOK("Get", err)
	})

	check("Admin Upsert", func() {
		_, err := clientAdmin.Upsert(ctx, &pb.UpsertRequest{
			CollectionName: "rbac_test", Vectors: []*pb.VectorWithId{makeVector(100)},
		})
		expectOK("Upsert", err)
	})

	check("Admin Delete", func() {
		_, err := clientAdmin.Delete(ctx, &pb.DeleteRequest{
			CollectionName: "rbac_test", Ids: []uint64{100},
		})
		expectOK("Delete", err)
	})

	check("Admin UpdateMetadata", func() {
		_, err := clientAdmin.UpdateMetadata(ctx, &pb.UpdateMetadataRequest{
			CollectionName: "rbac_test", Id: 1,
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"tag": {Value: &pb.MetadataValue_StringValue{StringValue: "test"}},
			}},
		})
		expectOK("UpdateMetadata", err)
	})

	check("Admin RangeSearch", func() {
		_, err := clientAdmin.RangeSearch(ctx, &pb.RangeSearchRequest{
			CollectionName: "rbac_test", QueryVector: qv, Radius: 1000.0,
		})
		expectOK("RangeSearch", err)
	})

	check("Admin HybridSearch", func() {
		_, err := clientAdmin.HybridSearch(ctx, &pb.HybridSearchRequest{
			CollectionName: "rbac_test", QueryVector: qv, TopK: 5,
		})
		expectOK("HybridSearch", err)
	})

	check("Admin ListVectors", func() {
		_, err := clientAdmin.ListVectors(ctx, &pb.ListVectorsRequest{
			CollectionName: "rbac_test", Limit: 5,
		})
		expectOK("ListVectors", err)
	})

	check("Admin ListCollections", func() {
		_, err := clientAdmin.ListCollections(ctx, &pb.ListCollectionsRequest{})
		expectOK("ListCollections", err)
	})

	// ====================================================================
	fmt.Println("\n--- Legacy api_keys backward compat ---")
	// ====================================================================

	check("Legacy key treated as admin (CreateCollection)", func() {
		_, err := clientLegacy.CreateCollection(ctx, &pb.CreateCollectionRequest{
			CollectionName: "legacy_coll", Dimension: 4,
			Metric: pb.CreateCollectionRequest_L2, IndexType: pb.CreateCollectionRequest_FLAT,
		})
		expectOK("Legacy CreateCollection", err)
	})

	check("Legacy key DropCollection", func() {
		_, err := clientLegacy.DropCollection(ctx, &pb.DropCollectionRequest{
			CollectionName: "legacy_coll",
		})
		expectOK("Legacy DropCollection", err)
	})

	// ====================================================================
	fmt.Println("\n--- Readwrite role ---")
	// ====================================================================

	check("Readwrite cannot CreateCollection", func() {
		_, err := clientRw.CreateCollection(ctx, &pb.CreateCollectionRequest{
			CollectionName: "rw_blocked", Dimension: 4,
			Metric: pb.CreateCollectionRequest_L2, IndexType: pb.CreateCollectionRequest_FLAT,
		})
		expectCode("CreateCollection", err, codes.PermissionDenied)
	})

	check("Readwrite cannot DropCollection", func() {
		_, err := clientRw.DropCollection(ctx, &pb.DropCollectionRequest{
			CollectionName: "rbac_test",
		})
		expectCode("DropCollection", err, codes.PermissionDenied)
	})

	check("Readwrite can Insert on allowed collection", func() {
		_, err := clientRw.Insert(ctx, &pb.InsertRequest{
			CollectionName: "rbac_test", Vectors: []*pb.VectorWithId{makeVector(200)},
		})
		expectOK("Insert", err)
	})

	check("Readwrite can Upsert on allowed collection", func() {
		_, err := clientRw.Upsert(ctx, &pb.UpsertRequest{
			CollectionName: "rbac_test", Vectors: []*pb.VectorWithId{makeVector(201)},
		})
		expectOK("Upsert", err)
	})

	check("Readwrite can Delete on allowed collection", func() {
		_, err := clientRw.Delete(ctx, &pb.DeleteRequest{
			CollectionName: "rbac_test", Ids: []uint64{200},
		})
		expectOK("Delete", err)
	})

	check("Readwrite can Search on allowed collection", func() {
		_, err := clientRw.Search(ctx, &pb.SearchRequest{
			CollectionName: "rbac_test", QueryVector: qv, TopK: 5,
		})
		expectOK("Search", err)
	})

	check("Readwrite can Get on allowed collection", func() {
		_, err := clientRw.Get(ctx, &pb.GetRequest{
			CollectionName: "rbac_test", Ids: []uint64{1},
		})
		expectOK("Get", err)
	})

	check("Readwrite can UpdateMetadata on allowed collection", func() {
		_, err := clientRw.UpdateMetadata(ctx, &pb.UpdateMetadataRequest{
			CollectionName: "rbac_test", Id: 1,
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"rw": {Value: &pb.MetadataValue_StringValue{StringValue: "yes"}},
			}},
		})
		expectOK("UpdateMetadata", err)
	})

	check("Readwrite can ListCollections", func() {
		_, err := clientRw.ListCollections(ctx, &pb.ListCollectionsRequest{})
		expectOK("ListCollections", err)
	})

	check("Readwrite denied on wrong collection (Insert)", func() {
		_, err := clientRw.Insert(ctx, &pb.InsertRequest{
			CollectionName: "other_coll", Vectors: []*pb.VectorWithId{makeVector(1)},
		})
		expectCode("Insert other_coll", err, codes.PermissionDenied)
	})

	check("Readwrite denied on wrong collection (Search)", func() {
		_, err := clientRw.Search(ctx, &pb.SearchRequest{
			CollectionName: "other_coll", QueryVector: qv, TopK: 5,
		})
		expectCode("Search other_coll", err, codes.PermissionDenied)
	})

	check("Readwrite denied on wrong collection (Delete)", func() {
		_, err := clientRw.Delete(ctx, &pb.DeleteRequest{
			CollectionName: "other_coll", Ids: []uint64{1},
		})
		expectCode("Delete other_coll", err, codes.PermissionDenied)
	})

	// Readwrite has two allowed collections
	check("Readwrite can access second allowed collection", func() {
		clientAdmin.CreateCollection(ctx, &pb.CreateCollectionRequest{
			CollectionName: "second_coll", Dimension: 4,
			Metric: pb.CreateCollectionRequest_L2, IndexType: pb.CreateCollectionRequest_FLAT,
		})
		_, err := clientRw.Insert(ctx, &pb.InsertRequest{
			CollectionName: "second_coll", Vectors: []*pb.VectorWithId{makeVector(1)},
		})
		expectOK("Insert second_coll", err)
	})

	// ====================================================================
	fmt.Println("\n--- Readonly role ---")
	// ====================================================================

	check("Readonly cannot Insert", func() {
		_, err := clientRo.Insert(ctx, &pb.InsertRequest{
			CollectionName: "rbac_test", Vectors: []*pb.VectorWithId{makeVector(300)},
		})
		expectCode("Insert", err, codes.PermissionDenied)
	})

	check("Readonly cannot Upsert", func() {
		_, err := clientRo.Upsert(ctx, &pb.UpsertRequest{
			CollectionName: "rbac_test", Vectors: []*pb.VectorWithId{makeVector(300)},
		})
		expectCode("Upsert", err, codes.PermissionDenied)
	})

	check("Readonly cannot Delete", func() {
		_, err := clientRo.Delete(ctx, &pb.DeleteRequest{
			CollectionName: "rbac_test", Ids: []uint64{1},
		})
		expectCode("Delete", err, codes.PermissionDenied)
	})

	check("Readonly cannot UpdateMetadata", func() {
		_, err := clientRo.UpdateMetadata(ctx, &pb.UpdateMetadataRequest{
			CollectionName: "rbac_test", Id: 1,
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"ro": {Value: &pb.MetadataValue_StringValue{StringValue: "no"}},
			}},
		})
		expectCode("UpdateMetadata", err, codes.PermissionDenied)
	})

	check("Readonly cannot CreateCollection", func() {
		_, err := clientRo.CreateCollection(ctx, &pb.CreateCollectionRequest{
			CollectionName: "ro_blocked", Dimension: 4,
			Metric: pb.CreateCollectionRequest_L2, IndexType: pb.CreateCollectionRequest_FLAT,
		})
		expectCode("CreateCollection", err, codes.PermissionDenied)
	})

	check("Readonly cannot DropCollection", func() {
		_, err := clientRo.DropCollection(ctx, &pb.DropCollectionRequest{
			CollectionName: "rbac_test",
		})
		expectCode("DropCollection", err, codes.PermissionDenied)
	})

	check("Readonly can Search", func() {
		_, err := clientRo.Search(ctx, &pb.SearchRequest{
			CollectionName: "rbac_test", QueryVector: qv, TopK: 5,
		})
		expectOK("Search", err)
	})

	check("Readonly can Get", func() {
		_, err := clientRo.Get(ctx, &pb.GetRequest{
			CollectionName: "rbac_test", Ids: []uint64{1, 2},
		})
		expectOK("Get", err)
	})

	check("Readonly can RangeSearch", func() {
		_, err := clientRo.RangeSearch(ctx, &pb.RangeSearchRequest{
			CollectionName: "rbac_test", QueryVector: qv, Radius: 1000.0,
		})
		expectOK("RangeSearch", err)
	})

	check("Readonly can HybridSearch", func() {
		_, err := clientRo.HybridSearch(ctx, &pb.HybridSearchRequest{
			CollectionName: "rbac_test", QueryVector: qv, TopK: 5,
		})
		expectOK("HybridSearch", err)
	})

	check("Readonly can ListVectors", func() {
		_, err := clientRo.ListVectors(ctx, &pb.ListVectorsRequest{
			CollectionName: "rbac_test", Limit: 5,
		})
		expectOK("ListVectors", err)
	})

	check("Readonly can ListCollections", func() {
		_, err := clientRo.ListCollections(ctx, &pb.ListCollectionsRequest{})
		expectOK("ListCollections", err)
	})

	// ====================================================================
	fmt.Println("\n--- Collection admin role ---")
	// ====================================================================

	check("CollectionAdmin cannot CreateCollection", func() {
		_, err := clientCa.CreateCollection(ctx, &pb.CreateCollectionRequest{
			CollectionName: "ca_blocked", Dimension: 4,
			Metric: pb.CreateCollectionRequest_L2, IndexType: pb.CreateCollectionRequest_FLAT,
		})
		expectCode("CreateCollection", err, codes.PermissionDenied)
	})

	check("CollectionAdmin cannot DropCollection", func() {
		_, err := clientCa.DropCollection(ctx, &pb.DropCollectionRequest{
			CollectionName: "rbac_test",
		})
		expectCode("DropCollection", err, codes.PermissionDenied)
	})

	check("CollectionAdmin can Insert on own collection", func() {
		_, err := clientCa.Insert(ctx, &pb.InsertRequest{
			CollectionName: "rbac_test", Vectors: []*pb.VectorWithId{makeVector(400)},
		})
		expectOK("Insert", err)
	})

	check("CollectionAdmin can Upsert on own collection", func() {
		_, err := clientCa.Upsert(ctx, &pb.UpsertRequest{
			CollectionName: "rbac_test", Vectors: []*pb.VectorWithId{makeVector(401)},
		})
		expectOK("Upsert", err)
	})

	check("CollectionAdmin can Delete on own collection", func() {
		_, err := clientCa.Delete(ctx, &pb.DeleteRequest{
			CollectionName: "rbac_test", Ids: []uint64{400},
		})
		expectOK("Delete", err)
	})

	check("CollectionAdmin can Search on own collection", func() {
		_, err := clientCa.Search(ctx, &pb.SearchRequest{
			CollectionName: "rbac_test", QueryVector: qv, TopK: 5,
		})
		expectOK("Search", err)
	})

	check("CollectionAdmin can Get on own collection", func() {
		_, err := clientCa.Get(ctx, &pb.GetRequest{
			CollectionName: "rbac_test", Ids: []uint64{1},
		})
		expectOK("Get", err)
	})

	check("CollectionAdmin can UpdateMetadata on own collection", func() {
		_, err := clientCa.UpdateMetadata(ctx, &pb.UpdateMetadataRequest{
			CollectionName: "rbac_test", Id: 1,
			Metadata: &pb.Metadata{Fields: map[string]*pb.MetadataValue{
				"ca": {Value: &pb.MetadataValue_StringValue{StringValue: "yes"}},
			}},
		})
		expectOK("UpdateMetadata", err)
	})

	check("CollectionAdmin denied on other collection (Insert)", func() {
		_, err := clientCa.Insert(ctx, &pb.InsertRequest{
			CollectionName: "other_coll", Vectors: []*pb.VectorWithId{makeVector(1)},
		})
		expectCode("Insert other_coll", err, codes.PermissionDenied)
	})

	check("CollectionAdmin denied on other collection (Search)", func() {
		_, err := clientCa.Search(ctx, &pb.SearchRequest{
			CollectionName: "other_coll", QueryVector: qv, TopK: 5,
		})
		expectCode("Search other_coll", err, codes.PermissionDenied)
	})

	check("CollectionAdmin denied on other collection (Delete)", func() {
		_, err := clientCa.Delete(ctx, &pb.DeleteRequest{
			CollectionName: "other_coll", Ids: []uint64{1},
		})
		expectCode("Delete other_coll", err, codes.PermissionDenied)
	})

	// ====================================================================
	fmt.Println("\n--- Admin DropCollection (cleanup) ---")
	// ====================================================================

	check("Admin DropCollection rbac_test", func() {
		_, err := clientAdmin.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: "rbac_test"})
		expectOK("DropCollection rbac_test", err)
	})

	check("Admin DropCollection other_coll", func() {
		_, err := clientAdmin.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: "other_coll"})
		expectOK("DropCollection other_coll", err)
	})

	check("Admin DropCollection second_coll", func() {
		_, err := clientAdmin.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: "second_coll"})
		expectOK("DropCollection second_coll", err)
	})

	// ====================================================================
	fmt.Printf("\n======================================================================\n")
	fmt.Printf("RBAC E2E: %d/%d checks passed\n", passed, total)
	fmt.Println("======================================================================")

	if passed != total {
		log.Fatalf("RBAC E2E Test FAILED: %d/%d", passed, total)
	}
	fmt.Println("RBAC E2E Test PASSED")
	fmt.Println("======================================================================")
}
