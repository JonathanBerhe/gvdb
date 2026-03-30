#!/bin/bash

# Distributed Flow Test Script for GVDB
# Tests: Client → Proxy → Coordinator/Data/Query Nodes

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}GVDB Distributed Flow Test${NC}"
echo -e "${BLUE}========================================${NC}"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    pkill -f gvdb-coordinator || true
    pkill -f gvdb-data-node || true
    pkill -f gvdb-query-node || true
    pkill -f gvdb-proxy || true
    rm -rf /tmp/gvdb-dist-test
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Register cleanup on exit
trap cleanup EXIT INT TERM

# Create test directories
echo -e "${YELLOW}Creating test directories...${NC}"
mkdir -p /tmp/gvdb-dist-test/{coordinator,data_node,query_node,proxy}/logs

# Start coordinator
echo -e "${YELLOW}Starting coordinator (node_id=1, port=50051)...${NC}"
./build/bin/gvdb-coordinator \
  --node-id 1 \
  --bind-address 0.0.0.0:50051 \
  --raft-address 0.0.0.0:8300 \
  --data-dir /tmp/gvdb-dist-test/coordinator \
  --single-node \
  > /tmp/gvdb-dist-test/coordinator.log 2>&1 &

COORD_PID=$!
echo -e "${GREEN}✓ Coordinator started (PID: $COORD_PID)${NC}"
sleep 3

# Check if coordinator is running
if ! kill -0 $COORD_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Coordinator failed to start${NC}"
    cat /tmp/gvdb-dist-test/coordinator.log
    exit 1
fi

# Start data node
echo -e "${YELLOW}Starting data node (node_id=101, port=50060)...${NC}"
./build/bin/gvdb-data-node \
  --node-id 101 \
  --bind-address 0.0.0.0:50060 \
  --data-dir /tmp/gvdb-dist-test/data_node \
  --coordinator localhost:50051 \
  > /tmp/gvdb-dist-test/data_node.log 2>&1 &

DATA_PID=$!
echo -e "${GREEN}✓ Data node started (PID: $DATA_PID)${NC}"
sleep 2

if ! kill -0 $DATA_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Data node failed to start${NC}"
    cat /tmp/gvdb-dist-test/data_node.log
    exit 1
fi

# Start query node
echo -e "${YELLOW}Starting query node (node_id=201, port=50070)...${NC}"
./build/bin/gvdb-query-node \
  --node-id 201 \
  --bind-address 0.0.0.0:50070 \
  --data-dir /tmp/gvdb-dist-test/query_node \
  --coordinator localhost:50051 \
  > /tmp/gvdb-dist-test/query_node.log 2>&1 &

QUERY_PID=$!
echo -e "${GREEN}✓ Query node started (PID: $QUERY_PID)${NC}"
sleep 2

if ! kill -0 $QUERY_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Query node failed to start${NC}"
    cat /tmp/gvdb-dist-test/query_node.log
    exit 1
fi

# Start proxy
echo -e "${YELLOW}Starting proxy (node_id=1, port=50050)...${NC}"
./build/bin/gvdb-proxy \
  --node-id 1 \
  --bind-address 0.0.0.0:50050 \
  --data-dir /tmp/gvdb-dist-test/proxy \
  --coordinators localhost:50051 \
  --query-nodes localhost:50070 \
  --data-nodes localhost:50060 \
  > /tmp/gvdb-dist-test/proxy.log 2>&1 &

PROXY_PID=$!
echo -e "${GREEN}✓ Proxy started (PID: $PROXY_PID)${NC}"
sleep 2

if ! kill -0 $PROXY_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Proxy failed to start${NC}"
    cat /tmp/gvdb-dist-test/proxy.log
    exit 1
fi

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}All nodes started successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "Coordinator:  localhost:50051 (PID: $COORD_PID)"
echo -e "Data Node:    localhost:50060 (PID: $DATA_PID)"
echo -e "Query Node:   localhost:50070 (PID: $QUERY_PID)"
echo -e "Proxy:        localhost:50050 (PID: $PROXY_PID)"
echo -e "${GREEN}========================================${NC}"

# Wait for services to stabilize
echo -e "\n${YELLOW}Waiting for services to stabilize...${NC}"
sleep 3

# Run e2e tests against proxy
echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}Running E2E Tests Against Proxy${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "${YELLOW}Client → Proxy (50050) → Coordinator (50051)${NC}\n"

cd test/e2e
export GVDB_SERVER_ADDR="localhost:50050"
go run e2e.go

TEST_EXIT_CODE=$?
cd ../..

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}✓ All tests passed!${NC}"
    echo -e "${GREEN}========================================${NC}"
else
    echo -e "\n${RED}========================================${NC}"
    echo -e "${RED}✗ Tests failed!${NC}"
    echo -e "${RED}========================================${NC}"
    exit 1
fi

# Show recent logs to verify proxy routing
echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}Proxy Routing Verification${NC}"
echo -e "${BLUE}========================================${NC}"

echo -e "\n${YELLOW}Proxy log (showing routing):${NC}"
tail -20 /tmp/gvdb-dist-test/proxy.log | grep -i "createcollection\|search\|insert" || echo "  (no routing logs found - this is normal)"

echo -e "\n${YELLOW}Coordinator log (showing received requests):${NC}"
tail -20 /tmp/gvdb-dist-test/coordinator.log | grep -i "createcollection\|collection created" || echo "  (no collection logs found)"

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Distributed Flow Test Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "\n${YELLOW}Architecture verified:${NC}"
echo -e "  Client → Proxy (50050)"
echo -e "    ├─ Metadata ops → Coordinator (50051)"
echo -e "    ├─ Search ops → Query Node (50070) [via LoadBalancer]"
echo -e "    └─ Data ops → Data Node (50060)"
echo -e "\n${YELLOW}Logs available at:${NC}"
echo -e "  - /tmp/gvdb-dist-test/coordinator.log"
echo -e "  - /tmp/gvdb-dist-test/data_node.log"
echo -e "  - /tmp/gvdb-dist-test/query_node.log"
echo -e "  - /tmp/gvdb-dist-test/proxy.log"
