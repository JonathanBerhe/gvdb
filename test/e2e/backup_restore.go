// Backup / restore end-to-end against a running single-node binary.
//
// Drives the real client-facing gRPC surface: CreateCollection, Insert,
// BackupCollection, GetBackupStatus, DropCollection, RestoreCollection,
// GetRestoreStatus, Search. Validates that vectors inserted before the
// backup are queryable after the restore from a freshly-named target
// collection.
//
// Two modes:
//   1. S3 target via MinIO. Requires GVDB_S3_ENDPOINT and that the
//      server was started with S3 wired (the `make test-s3` harness
//      does this). Skipped otherwise — same convention as
//      s3_storage.go.
//   2. Local target. Requires GVDB_LOCAL_BACKUP_DIR pointing at a
//      directory the server was configured to allow (matches
//      storage.local_backup_dir in the server's YAML).
//
// Run via run_all_tests.sh after starting MinIO + a configured server.
// Standalone:
//   make test-s3
// or, for the local-target lane:
//   GVDB_LOCAL_BACKUP_DIR=/tmp/gvdb-backup-e2e \
//     go run test/e2e/backup_restore.go test/e2e/helpers.go ...

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	pb "gvdb/integration-tests/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func waitForBackup(ctx context.Context, client pb.VectorDBServiceClient,
	id string, timeout time.Duration) (pb.BackupState, string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.GetBackupStatus(ctx,
			&pb.GetBackupStatusRequest{BackupId: id})
		if err != nil {
			return pb.BackupState_BACKUP_FAILED, "", err
		}
		switch resp.State {
		case pb.BackupState_BACKUP_COMPLETED,
			pb.BackupState_BACKUP_FAILED,
			pb.BackupState_BACKUP_CANCELLED:
			return resp.State, resp.ErrorMessage, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return pb.BackupState_BACKUP_RUNNING, "timeout", nil
}

func waitForRestore(ctx context.Context, client pb.VectorDBServiceClient,
	id string, timeout time.Duration) (pb.BackupState, string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.GetRestoreStatus(ctx,
			&pb.GetRestoreStatusRequest{RestoreId: id})
		if err != nil {
			return pb.BackupState_BACKUP_FAILED, "", err
		}
		switch resp.State {
		case pb.BackupState_BACKUP_COMPLETED,
			pb.BackupState_BACKUP_FAILED:
			return resp.State, resp.ErrorMessage, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return pb.BackupState_BACKUP_RUNNING, "timeout", nil
}

func runBackupRestoreTests() {
	s3Endpoint := os.Getenv("GVDB_S3_ENDPOINT")
	localDir := os.Getenv("GVDB_LOCAL_BACKUP_DIR")
	if s3Endpoint == "" && localDir == "" {
		fmt.Println("SKIP: backup/restore e2e requires GVDB_S3_ENDPOINT (MinIO) or GVDB_LOCAL_BACKUP_DIR")
		return
	}

	serverAddr := GetServerAddr()
	fmt.Println("=== Backup / Restore E2E Test ===")
	fmt.Printf("Server: %s\n", serverAddr)
	if s3Endpoint != "" {
		fmt.Printf("S3 endpoint: %s\n", s3Endpoint)
	}
	if localDir != "" {
		fmt.Printf("Local dir: %s\n", localDir)
	}

	conn, err := grpc.Dial(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("FAIL: Could not connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewVectorDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	suffix := time.Now().UnixNano()
	collection := fmt.Sprintf("bkp_e2e_%d", suffix)
	restored := fmt.Sprintf("bkp_e2e_restored_%d", suffix)
	const dim uint32 = 8
	const count = 32

	// 1. Create + populate the source collection.
	fmt.Printf("CreateCollection %s\n", collection)
	_, err = client.CreateCollection(ctx, &pb.CreateCollectionRequest{
		CollectionName: collection,
		Dimension:      dim,
		Metric:         pb.CreateCollectionRequest_L2,
		IndexType:      pb.CreateCollectionRequest_FLAT,
	})
	if err != nil {
		fmt.Printf("FAIL: CreateCollection: %v\n", err)
		os.Exit(1)
	}

	vectors := make([]*pb.VectorWithId, count)
	for i := 0; i < count; i++ {
		values := make([]float32, dim)
		for d := uint32(0); d < dim; d++ {
			values[d] = float32(i*10) + float32(d)
		}
		vectors[i] = &pb.VectorWithId{
			Id:     uint64(i + 1),
			Vector: &pb.Vector{Values: values, Dimension: dim},
		}
	}
	_, err = client.Insert(ctx,
		&pb.InsertRequest{CollectionName: collection, Vectors: vectors})
	if err != nil {
		fmt.Printf("FAIL: Insert: %v\n", err)
		os.Exit(1)
	}

	// 2. Backup with either an S3 or Local target.
	target := &pb.BackupTarget{}
	if s3Endpoint != "" {
		// The bucket comes from the server config; the e2e harness
		// configures "gvdb-test" for MinIO runs.
		target.Target = &pb.BackupTarget_S3{
			S3: &pb.S3Target{Bucket: "gvdb-test", Prefix: "e2e-backups"},
		}
	} else {
		target.Target = &pb.BackupTarget_Local{
			Local: &pb.LocalTarget{Path: localDir},
		}
	}

	fmt.Println("BackupCollection")
	bkpResp, err := client.BackupCollection(ctx, &pb.BackupCollectionRequest{
		CollectionName: collection,
		Target:         target,
	})
	if err != nil {
		fmt.Printf("FAIL: BackupCollection: %v\n", err)
		os.Exit(1)
	}
	bkpState, bkpErr, err := waitForBackup(ctx, client, bkpResp.BackupId,
		60*time.Second)
	if err != nil {
		fmt.Printf("FAIL: GetBackupStatus: %v\n", err)
		os.Exit(1)
	}
	if bkpState != pb.BackupState_BACKUP_COMPLETED {
		fmt.Printf("FAIL: backup state=%v msg=%s\n", bkpState, bkpErr)
		os.Exit(1)
	}
	fmt.Printf("   OK backup_id=%s\n", bkpResp.BackupId)

	// 3. Drop the source collection so the restore lands cleanly.
	_, _ = client.DropCollection(ctx,
		&pb.DropCollectionRequest{CollectionName: collection})

	// 4. Restore into a fresh collection name.
	fmt.Println("RestoreCollection")
	source := &pb.BackupTarget{}
	if s3Endpoint != "" {
		source.Target = &pb.BackupTarget_S3{
			S3: &pb.S3Target{Bucket: "gvdb-test", Prefix: "e2e-backups"},
		}
	} else {
		source.Target = &pb.BackupTarget_Local{
			Local: &pb.LocalTarget{Path: localDir},
		}
	}
	rstResp, err := client.RestoreCollection(ctx, &pb.RestoreCollectionRequest{
		Source:               source,
		BackupId:             bkpResp.BackupId,
		TargetCollectionName: restored,
	})
	if err != nil {
		fmt.Printf("FAIL: RestoreCollection: %v\n", err)
		os.Exit(1)
	}
	rstState, rstErr, err := waitForRestore(ctx, client, rstResp.RestoreId,
		60*time.Second)
	if err != nil {
		fmt.Printf("FAIL: GetRestoreStatus: %v\n", err)
		os.Exit(1)
	}
	if rstState != pb.BackupState_BACKUP_COMPLETED {
		fmt.Printf("FAIL: restore state=%v msg=%s\n", rstState, rstErr)
		os.Exit(1)
	}
	fmt.Printf("   OK restore_id=%s\n", rstResp.RestoreId)

	// 5. Search the restored collection for a known vector. Expect the
	//    nearest neighbour to be the query itself (id 7).
	queryValues := make([]float32, dim)
	for d := uint32(0); d < dim; d++ {
		queryValues[d] = float32(6*10) + float32(d)  // matches vector id 7
	}
	searchResp, err := client.Search(ctx, &pb.SearchRequest{
		CollectionName: restored,
		QueryVector:    &pb.Vector{Values: queryValues, Dimension: dim},
		TopK:           1,
	})
	if err != nil {
		fmt.Printf("FAIL: Search restored: %v\n", err)
		os.Exit(1)
	}
	if len(searchResp.Results) == 0 || searchResp.Results[0].Id != 7 {
		fmt.Printf("FAIL: top match expected id=7, got results=%v\n",
			searchResp.Results)
		os.Exit(1)
	}
	fmt.Println("   OK Search restored top match = id 7")

	fmt.Println("PASS: backup → restore round-trip verified end-to-end")
}

func main() {
	runBackupRestoreTests()
}
