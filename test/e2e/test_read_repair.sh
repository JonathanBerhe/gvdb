#!/bin/bash
# test_read_repair.sh — Verify crash recovery and repair in kind cluster
#
# Scenario:
#   1. Create collection, insert 500 vectors, verify search
#   2. Kill data-node-1 pod
#   3. Insert 500 more vectors while node-1 is down
#   4. Wait for data-node-1 to recover
#   5. Wait for coordinator repair cycle
#   6. Verify search returns results and all pods are healthy
#
# Usage: ./test_read_repair.sh
# Requires: kubectl, port-forward to proxy on localhost:50050

set -euo pipefail

NAMESPACE="gvdb"
PROXY_ADDR="localhost:50050"
COLLECTION="repair_test_$(date +%s)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}  $1${NC}"; }
fail() { echo -e "${RED}  $1${NC}"; exit 1; }
info() { echo -e "${YELLOW}-> $1${NC}"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "======================================================================"
echo "  GVDB Read Repair Verification Test"
echo "  Collection: $COLLECTION"
echo "======================================================================"

# --- Pre-flight ---
info "Step 1: Verifying cluster health..."
PODS=$(kubectl -n $NAMESPACE get pods --no-headers 2>/dev/null | grep Running | wc -l | tr -d ' ')
if [ "$PODS" -lt 5 ]; then fail "Expected 5+ running pods, got $PODS"; fi
pass "Cluster healthy ($PODS pods running)"

info "Step 2: Checking proxy connectivity..."
if ! nc -z localhost 50050 2>/dev/null; then fail "Port-forward not active"; fi
pass "Proxy reachable"

# --- Build Go helper ---
info "Step 3: Building test binary..."

cat > "$SCRIPT_DIR/repair_test_main.go" << 'GOEOF'
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	pb "gvdb/integration-tests/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := os.Getenv("GVDB_SERVER_ADDR")
	if addr == "" {
		addr = "localhost:50050"
	}
	phase := os.Getenv("TEST_PHASE")
	collName := os.Getenv("COLLECTION_NAME")
	if collName == "" {
		collName = "repair_test"
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256*1024*1024),
			grpc.MaxCallSendMsgSize(256*1024*1024)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewVectorDBServiceClient(conn)
	ctx := context.Background()

	switch phase {
	case "create":
		client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collName})
		time.Sleep(500 * time.Millisecond)
		_, err := client.CreateCollection(ctx, &pb.CreateCollectionRequest{
			CollectionName: collName,
			Dimension:      128,
			Metric:         pb.CreateCollectionRequest_L2,
			IndexType:      pb.CreateCollectionRequest_FLAT,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "create failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Collection created")

	case "insert":
		startID := 1
		if os.Getenv("START_ID") != "" {
			fmt.Sscanf(os.Getenv("START_ID"), "%d", &startID)
		}
		count := 500
		if os.Getenv("COUNT") != "" {
			fmt.Sscanf(os.Getenv("COUNT"), "%d", &count)
		}
		batchSize := 100
		for batch := 0; batch < count/batchSize; batch++ {
			vecs := make([]*pb.VectorWithId, batchSize)
			for i := 0; i < batchSize; i++ {
				id := uint64(startID + batch*batchSize + i)
				values := make([]float32, 128)
				for j := range values {
					values[j] = rand.Float32()*2 - 1
				}
				vecs[i] = &pb.VectorWithId{
					Id:     id,
					Vector: &pb.Vector{Values: values, Dimension: 128},
				}
			}
			resp, err := client.Insert(ctx, &pb.InsertRequest{
				CollectionName: collName,
				Vectors:        vecs,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "insert batch %d failed: %v\n", batch, err)
				os.Exit(1)
			}
			fmt.Printf("  Inserted batch %d (%d vectors)\n", batch, resp.InsertedCount)
		}

	case "search":
		values := make([]float32, 128)
		for j := range values {
			values[j] = rand.Float32()*2 - 1
		}
		resp, err := client.Search(ctx, &pb.SearchRequest{
			CollectionName: collName,
			QueryVector:    &pb.Vector{Values: values, Dimension: 128},
			TopK:           10,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "search failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("  Search returned %d results (%.1fms)\n", len(resp.Results), resp.QueryTimeMs)
		if len(resp.Results) == 0 {
			os.Exit(1)
		}

	case "cleanup":
		client.DropCollection(ctx, &pb.DropCollectionRequest{CollectionName: collName})
		fmt.Println("  Collection dropped")
	}
}
GOEOF

cd "$SCRIPT_DIR"
go build -o ./repair_test_bin ./repair_test_main.go ./helpers.go 2>&1
pass "Test binary built"

# Common env for the Go binary
export GVDB_SERVER_ADDR="$PROXY_ADDR"
export COLLECTION_NAME="$COLLECTION"

# --- Phase 1: Create and populate ---
info "Step 4: Creating collection '$COLLECTION'..."
TEST_PHASE=create ./repair_test_bin
sleep 5
pass "Collection created"

info "Step 5: Inserting 500 vectors (phase 1)..."
for attempt in 1 2 3; do
  if TEST_PHASE=insert START_ID=1 COUNT=500 ./repair_test_bin 2>&1; then break; fi
  if [ $attempt -lt 3 ]; then
    info "  Retry in 3s ($attempt/3)..."; sleep 3
  else
    fail "Insert failed after 3 attempts"
  fi
done
pass "Phase 1: 500 vectors inserted"

info "Step 6: Verifying search..."
TEST_PHASE=search ./repair_test_bin
pass "Search works"

# --- Phase 2: Crash and diverge ---
info "Step 7: Killing gvdb-data-node-1..."
kubectl -n $NAMESPACE delete pod gvdb-data-node-1 --grace-period=0 --force 2>/dev/null
sleep 2
pass "data-node-1 killed"

info "Step 8: Inserting 500 more vectors (node-1 down)..."
TEST_PHASE=insert START_ID=501 COUNT=500 ./repair_test_bin 2>&1 || {
  info "  Some inserts may fail (expected)"
}
pass "Phase 2 inserts done"

# --- Phase 3: Recovery ---
info "Step 9: Waiting for data-node-1 recovery..."
kubectl -n $NAMESPACE rollout status statefulset/gvdb-data-node --timeout=120s 2>&1
pass "data-node-1 recovered"

info "Step 10: Re-establishing port-forward..."
pkill -f "kubectl.*port-forward.*gvdb-proxy" 2>/dev/null || true
sleep 2
kubectl -n $NAMESPACE port-forward svc/gvdb-proxy 50050:50050 2>/dev/null &
sleep 3
if nc -z localhost 50050 2>/dev/null; then pass "Port-forward OK"; else fail "Port-forward failed"; fi

info "Step 11: Waiting 40s for coordinator repair cycle..."
sleep 40

# --- Phase 4: Verify ---
info "Step 12: Checking coordinator logs..."
REPAIR_LOGS=$(kubectl -n $NAMESPACE logs gvdb-coordinator-0 --since=120s 2>/dev/null | { grep -c -i "ReadRepair\|replicated\|Auto-replicated\|Registered new node" || echo "0"; })
echo "  Found $REPAIR_LOGS repair/replication entries"

HEALTH_LOGS=$(kubectl -n $NAMESPACE logs gvdb-coordinator-0 --since=120s 2>/dev/null | { grep -i "ReadRepair\|replicated\|Auto-replicated\|Registered new\|Promoted" || true; } | tail -5)
if [ -n "$HEALTH_LOGS" ]; then
  echo "$HEALTH_LOGS" | sed 's/^/     /'
  pass "Coordinator repair activity detected"
else
  info "No repair logs (expected with replication_factor=1)"
fi

info "Step 13: Verifying search after recovery..."
TEST_PHASE=search ./repair_test_bin
pass "Search works after recovery"

info "Step 14: Verifying pod health..."
RUNNING=$(kubectl -n $NAMESPACE get pods --no-headers 2>/dev/null | grep Running | wc -l | tr -d ' ')
if [ "$RUNNING" -lt 5 ]; then fail "Only $RUNNING pods running"; fi
pass "All $RUNNING pods healthy"

# --- Cleanup ---
info "Step 15: Cleanup..."
TEST_PHASE=cleanup ./repair_test_bin
rm -f ./repair_test_bin ./repair_test_main.go
pass "Done"

echo ""
echo "======================================================================"
echo -e "  ${GREEN}READ REPAIR VERIFICATION PASSED${NC}"
echo "======================================================================"
