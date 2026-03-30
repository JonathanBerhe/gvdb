#!/bin/bash

# Multi-Node Test Script for GVDB
# Tests coordinator + data node + query node + proxy with heartbeat protocol

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}GVDB Multi-Node Test${NC}"
echo -e "${GREEN}========================================${NC}"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    pkill -f gvdb-coordinator || true
    pkill -f gvdb-data-node || true
    pkill -f gvdb-query-node || true
    pkill -f gvdb-proxy || true
    rm -rf /tmp/gvdb-test
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Register cleanup on exit
trap cleanup EXIT INT TERM

# Create test directories
echo -e "${YELLOW}Creating test directories...${NC}"
mkdir -p /tmp/gvdb-test/{coordinator,data_node,query_node,proxy}/logs

# Start coordinator
echo -e "${YELLOW}Starting coordinator (node_id=1)...${NC}"
./build/bin/gvdb-coordinator \
  --node-id 1 \
  --bind-address 0.0.0.0:50051 \
  --raft-address 0.0.0.0:8300 \
  --data-dir /tmp/gvdb-test/coordinator \
  --single-node \
  > /tmp/gvdb-test/coordinator.log 2>&1 &

COORD_PID=$!
echo -e "${GREEN}Coordinator started (PID: $COORD_PID)${NC}"
sleep 2

# Check if coordinator is running
if ! kill -0 $COORD_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Coordinator failed to start${NC}"
    cat /tmp/gvdb-test/coordinator.log
    exit 1
fi

# Start data node
echo -e "${YELLOW}Starting data node (node_id=101)...${NC}"
./build/bin/gvdb-data-node \
  --node-id 101 \
  --bind-address 0.0.0.0:50060 \
  --data-dir /tmp/gvdb-test/data_node \
  --coordinator localhost:50051 \
  > /tmp/gvdb-test/data_node.log 2>&1 &

DATA_PID=$!
echo -e "${GREEN}Data node started (PID: $DATA_PID)${NC}"
sleep 2

# Check if data node is running
if ! kill -0 $DATA_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Data node failed to start${NC}"
    cat /tmp/gvdb-test/data_node.log
    exit 1
fi

# Start query node
echo -e "${YELLOW}Starting query node (node_id=201)...${NC}"
./build/bin/gvdb-query-node \
  --node-id 201 \
  --bind-address 0.0.0.0:50070 \
  --data-dir /tmp/gvdb-test/query_node \
  --coordinator localhost:50051 \
  > /tmp/gvdb-test/query_node.log 2>&1 &

QUERY_PID=$!
echo -e "${GREEN}Query node started (PID: $QUERY_PID)${NC}"
sleep 2

# Check if query node is running
if ! kill -0 $QUERY_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Query node failed to start${NC}"
    cat /tmp/gvdb-test/query_node.log
    exit 1
fi

# Start proxy
echo -e "${YELLOW}Starting proxy (node_id=1)...${NC}"
./build/bin/gvdb-proxy \
  --node-id 1 \
  --bind-address 0.0.0.0:50050 \
  --data-dir /tmp/gvdb-test/proxy \
  --coordinators localhost:50051 \
  --query-nodes localhost:50070 \
  --data-nodes localhost:50060 \
  > /tmp/gvdb-test/proxy.log 2>&1 &

PROXY_PID=$!
echo -e "${GREEN}Proxy started (PID: $PROXY_PID)${NC}"
sleep 2

# Check if proxy is running
if ! kill -0 $PROXY_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Proxy failed to start${NC}"
    cat /tmp/gvdb-test/proxy.log
    exit 1
fi

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}All nodes started successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "Coordinator: localhost:50051 (PID: $COORD_PID)"
echo -e "Data Node:   localhost:50060 (PID: $DATA_PID)"
echo -e "Query Node:  localhost:50070 (PID: $QUERY_PID)"
echo -e "Proxy:       localhost:50050 (PID: $PROXY_PID)"
echo -e "${GREEN}========================================${NC}"

# Monitor heartbeats for 30 seconds
echo -e "\n${YELLOW}Monitoring heartbeats for 30 seconds...${NC}"
echo -e "${YELLOW}(Heartbeats should appear every 10 seconds)${NC}\n"

for i in {1..30}; do
    echo -ne "${YELLOW}[$i/30] Monitoring... ${NC}\r"
    sleep 1

    # Check if all processes are still running
    if ! kill -0 $COORD_PID 2>/dev/null; then
        echo -e "\n${RED}ERROR: Coordinator died${NC}"
        exit 1
    fi
    if ! kill -0 $DATA_PID 2>/dev/null; then
        echo -e "\n${RED}ERROR: Data node died${NC}"
        exit 1
    fi
    if ! kill -0 $QUERY_PID 2>/dev/null; then
        echo -e "\n${RED}ERROR: Query node died${NC}"
        exit 1
    fi
    if ! kill -0 $PROXY_PID 2>/dev/null; then
        echo -e "\n${RED}ERROR: Proxy died${NC}"
        exit 1
    fi
done

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Test Results${NC}"
echo -e "${GREEN}========================================${NC}"

# Check for heartbeat messages in logs
echo -e "\n${YELLOW}Coordinator Log (last 20 lines):${NC}"
tail -20 /tmp/gvdb-test/coordinator.log | grep -i "heartbeat\|noderegistry" || echo "  (no heartbeat logs found)"

echo -e "\n${YELLOW}Data Node Log (last 10 lines):${NC}"
tail -10 /tmp/gvdb-test/data_node.log | grep -i "heartbeat" || echo "  (no heartbeat logs found)"

echo -e "\n${YELLOW}Query Node Log (last 10 lines):${NC}"
tail -10 /tmp/gvdb-test/query_node.log | grep -i "heartbeat" || echo "  (no heartbeat logs found)"

echo -e "\n${YELLOW}Proxy Log (last 10 lines):${NC}"
tail -10 /tmp/gvdb-test/proxy.log || echo "  (no proxy logs found)"

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Multi-node test completed successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "\n${YELLOW}Logs available at:${NC}"
echo -e "  - /tmp/gvdb-test/coordinator.log"
echo -e "  - /tmp/gvdb-test/data_node.log"
echo -e "  - /tmp/gvdb-test/query_node.log"
echo -e "  - /tmp/gvdb-test/proxy.log"
echo -e "\n${YELLOW}Press Ctrl+C to stop all nodes and cleanup${NC}"

# Keep running until user interrupts
while true; do
    sleep 1
done
