#!/bin/bash

# Test Advanced Load Suite Against Distributed Setup

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}GVDB Distributed Advanced Load Test${NC}"
echo -e "${GREEN}========================================${NC}"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    pkill -f gvdb-coordinator || true
    pkill -f gvdb-data-node || true
    pkill -f gvdb-query-node || true
    pkill -f gvdb-proxy || true
    rm -rf /tmp/gvdb-distributed-test
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Register cleanup on exit
# trap cleanup EXIT INT TERM  # DISABLED for debugging

# Create test directories
echo -e "${YELLOW}Creating test directories...${NC}"
mkdir -p /tmp/gvdb-distributed-test/{coordinator,data_node,query_node,proxy}

# Start coordinator
echo -e "${YELLOW}Starting coordinator...${NC}"
../../build/bin/gvdb-coordinator \
  --node-id 1 \
  --bind-address 0.0.0.0:50051 \
  --raft-address 0.0.0.0:8300 \
  --data-dir /tmp/gvdb-distributed-test/coordinator \
  --single-node \
  > /tmp/gvdb-distributed-test/coordinator.log 2>&1 &

COORD_PID=$!
echo -e "${GREEN}Coordinator started (PID: $COORD_PID)${NC}"
sleep 3

# Check if coordinator is running
if ! kill -0 $COORD_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Coordinator failed to start${NC}"
    cat /tmp/gvdb-distributed-test/coordinator.log
    exit 1
fi

# Start data node
echo -e "${YELLOW}Starting data node...${NC}"
../../build/bin/gvdb-data-node \
  --node-id 101 \
  --bind-address 0.0.0.0:50060 \
  --data-dir /tmp/gvdb-distributed-test/data_node \
  --coordinator localhost:50051 \
  > /tmp/gvdb-distributed-test/data_node.log 2>&1 &

DATA_PID=$!
echo -e "${GREEN}Data node started (PID: $DATA_PID)${NC}"
sleep 3

# Check if data node is running
if ! kill -0 $DATA_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Data node failed to start${NC}"
    cat /tmp/gvdb-distributed-test/data_node.log
    exit 1
fi

# Start query node
echo -e "${YELLOW}Starting query node...${NC}"
../../build/bin/gvdb-query-node \
  --node-id 201 \
  --bind-address 0.0.0.0:50070 \
  --data-dir /tmp/gvdb-distributed-test/query_node \
  --coordinator localhost:50051 \
  > /tmp/gvdb-distributed-test/query_node.log 2>&1 &

QUERY_PID=$!
echo -e "${GREEN}Query node started (PID: $QUERY_PID)${NC}"
sleep 3

# Check if query node is running
if ! kill -0 $QUERY_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Query node failed to start${NC}"
    cat /tmp/gvdb-distributed-test/query_node.log
    exit 1
fi

# Start proxy
echo -e "${YELLOW}Starting proxy...${NC}"
../../build/bin/gvdb-proxy \
  --node-id 1 \
  --bind-address 0.0.0.0:50050 \
  --data-dir /tmp/gvdb-distributed-test/proxy \
  --coordinators localhost:50051 \
  --query-nodes localhost:50070 \
  --data-nodes localhost:50060 \
  > /tmp/gvdb-distributed-test/proxy.log 2>&1 &

PROXY_PID=$!
echo -e "${GREEN}Proxy started (PID: $PROXY_PID)${NC}"
sleep 3

# Check if proxy is running
if ! kill -0 $PROXY_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Proxy failed to start${NC}"
    cat /tmp/gvdb-distributed-test/proxy.log
    exit 1
fi

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}All nodes started successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "Coordinator: localhost:50051"
echo -e "Data Node:   localhost:50060"
echo -e "Query Node:  localhost:50070"
echo -e "Proxy:       localhost:50050"
echo -e "${GREEN}========================================${NC}"

# Wait for system to stabilize
echo -e "\n${YELLOW}Waiting for system to stabilize (10 seconds)...${NC}"
sleep 10

# Run advanced load test against proxy (port 50050)
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Running Advanced Load Test${NC}"
echo -e "${GREEN}========================================${NC}\n"

# Build and run the load test pointing to proxy
export SERVER_ADDR=localhost:50050
./load_advanced 2>&1 | tee /tmp/gvdb-distributed-test/load_test_results.log

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Test Results Available At:${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "  - /tmp/gvdb-distributed-test/coordinator.log"
echo -e "  - /tmp/gvdb-distributed-test/data_node.log"
echo -e "  - /tmp/gvdb-distributed-test/query_node.log"
echo -e "  - /tmp/gvdb-distributed-test/proxy.log"
echo -e "  - /tmp/gvdb-distributed-test/load_test_results.log"
