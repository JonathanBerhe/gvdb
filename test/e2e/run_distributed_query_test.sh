#!/bin/bash

# run_distributed_query_test.sh
# Script to run the distributed query E2E test with coordinator + data node

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$PROJECT_ROOT"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================="
echo "GVDB Distributed Query E2E Test"
echo "========================================="

# Clean up any existing test directories and processes
echo -e "${YELLOW}Cleaning up...${NC}"
rm -rf /tmp/gvdb-e2e-coordinator /tmp/gvdb-e2e-datanode
pkill -f "gvdb-coordinator.*50051" || true
pkill -f "gvdb-data-node.*50052" || true
sleep 2

# Start coordinator on port 50051
echo -e "${YELLOW}Starting coordinator on localhost:50051...${NC}"
./build/bin/gvdb-coordinator \
  --node-id 1 \
  --bind-address localhost:50051 \
  --data-dir /tmp/gvdb-e2e-coordinator \
  > /tmp/gvdb-coordinator.log 2>&1 &
COORD_PID=$!
echo "Coordinator PID: $COORD_PID"

# Wait for coordinator to start
sleep 3

# Check if coordinator is running
if ! kill -0 $COORD_PID 2>/dev/null; then
    echo -e "${RED}❌ Coordinator failed to start. Logs:${NC}"
    cat /tmp/gvdb-coordinator.log
    exit 1
fi

echo -e "${GREEN}✓ Coordinator started${NC}"
echo "Coordinator logs:"
tail -5 /tmp/gvdb-coordinator.log
echo ""

# Start data node on port 50052
echo -e "${YELLOW}Starting data node on localhost:50052...${NC}"
./build/bin/gvdb-data-node \
  --node-id 2 \
  --bind-address localhost:50052 \
  --coordinator localhost:50051 \
  --shards 1,2,3,4,5,6,7,8 \
  --data-dir /tmp/gvdb-e2e-datanode \
  > /tmp/gvdb-datanode.log 2>&1 &
DATA_PID=$!
echo "Data node PID: $DATA_PID"

# Wait for data node to start and register and become healthy
sleep 5

# Check if data node is running
if ! kill -0 $DATA_PID 2>/dev/null; then
    echo -e "${RED}❌ Data node failed to start. Logs:${NC}"
    cat /tmp/gvdb-datanode.log
    echo -e "${YELLOW}Coordinator logs:${NC}"
    cat /tmp/gvdb-coordinator.log
    kill $COORD_PID 2>/dev/null || true
    exit 1
fi

echo -e "${GREEN}✓ Data node started${NC}"
echo "Data node logs:"
tail -5 /tmp/gvdb-datanode.log
echo ""

# Run the distributed query test
echo -e "${YELLOW}Running distributed query test...${NC}"
echo ""

if ./test/e2e/distributed_query localhost:50051 localhost:50052; then
    echo ""
    echo -e "${GREEN}=========================================${NC}"
    echo -e "${GREEN}✅ Distributed Query E2E Test PASSED!${NC}"
    echo -e "${GREEN}=========================================${NC}"
    TEST_RESULT=0
else
    echo ""
    echo -e "${RED}=========================================${NC}"
    echo -e "${RED}❌ Distributed Query E2E Test FAILED${NC}"
    echo -e "${RED}=========================================${NC}"
    echo ""
    echo -e "${YELLOW}Coordinator logs:${NC}"
    tail -20 /tmp/gvdb-coordinator.log
    echo ""
    echo -e "${YELLOW}Data node logs:${NC}"
    tail -20 /tmp/gvdb-datanode.log
    TEST_RESULT=1
fi

# Cleanup
echo ""
echo -e "${YELLOW}Cleaning up processes...${NC}"
kill $COORD_PID $DATA_PID 2>/dev/null || true
sleep 1

# Force kill if still running
pkill -9 -f "gvdb-coordinator.*50051" 2>/dev/null || true
pkill -9 -f "gvdb-data-node.*50052" 2>/dev/null || true

exit $TEST_RESULT
