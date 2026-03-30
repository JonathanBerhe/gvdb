#!/bin/bash

# Minimal test to reproduce IVF_PQ crash
# This test:
# 1. Starts coordinator + data node
# 2. Creates FLAT collection → drops it
# 3. Creates IVF_PQ collection → crashes

set -e

# Enable core dumps
ulimit -c unlimited

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Minimal IVF_PQ Crash Test${NC}"
echo -e "${GREEN}========================================${NC}"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    pkill -f gvdb-coordinator || true
    pkill -f gvdb-data-node || true
    rm -rf /tmp/gvdb-crash-test
}

trap cleanup EXIT INT TERM

# Create test directories
mkdir -p /tmp/gvdb-crash-test/{coordinator,data_node}

# Start coordinator
echo -e "${YELLOW}Starting coordinator...${NC}"
../../build/bin/gvdb-coordinator \
  --node-id 1 \
  --bind-address 0.0.0.0:50051 \
  --raft-address 0.0.0.0:8300 \
  --data-dir /tmp/gvdb-crash-test/coordinator \
  --single-node \
  > /tmp/gvdb-crash-test/coordinator.log 2>&1 &

COORD_PID=$!
echo "Coordinator PID: $COORD_PID"
sleep 3

# Check if coordinator is running
if ! kill -0 $COORD_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Coordinator failed to start${NC}"
    cat /tmp/gvdb-crash-test/coordinator.log
    exit 1
fi

# Start data node
echo -e "${YELLOW}Starting data node...${NC}"
../../build/bin/gvdb-data-node \
  --node-id 101 \
  --bind-address 0.0.0.0:50060 \
  --data-dir /tmp/gvdb-crash-test/data_node \
  --coordinator localhost:50051 \
  > /tmp/gvdb-crash-test/data_node.log 2>&1 &

DATA_PID=$!
echo "Data node PID: $DATA_PID"
sleep 3

# Check if data node is running
if ! kill -0 $DATA_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Data node failed to start${NC}"
    cat /tmp/gvdb-crash-test/data_node.log
    exit 1
fi

echo -e "${GREEN}Nodes started successfully${NC}"
sleep 5

# Build test client
echo -e "${YELLOW}Building test client...${NC}"
go build -o test_crash_client test_crash_client.go

# Run test
echo -e "${YELLOW}Running test...${NC}"
./test_crash_client

# Check if coordinator crashed
sleep 2
if ! kill -0 $COORD_PID 2>/dev/null; then
    echo -e "${RED}COORDINATOR CRASHED!${NC}"
    echo "Coordinator log:"
    tail -50 /tmp/gvdb-crash-test/coordinator.log
    exit 1
else
    echo -e "${GREEN}Test completed successfully!${NC}"
fi
