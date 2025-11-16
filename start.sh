#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}GVDB Complete Startup Script${NC}"
echo -e "${GREEN}========================================${NC}"

# Step 1: Build GVDB
echo -e "\n${YELLOW}[1/5] Building GVDB...${NC}"
if [ ! -d "build" ]; then
    echo "Creating build directory..."
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
fi

cmake --build build -j4
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Build successful${NC}"
else
    echo -e "${RED}✗ Build failed${NC}"
    exit 1
fi

# Step 2: Run tests
echo -e "\n${YELLOW}[2/5] Running tests...${NC}"
ctest --test-dir build --output-on-failure
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed${NC}"
else
    echo -e "${RED}✗ Tests failed${NC}"
    exit 1
fi

# Step 3: Start Docker Compose (Prometheus + Grafana)
echo -e "\n${YELLOW}[3/5] Starting Prometheus + Grafana...${NC}"
cd grafana
docker compose down 2>/dev/null || true
docker compose up -d
cd ..

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Monitoring stack started${NC}"
else
    echo -e "${RED}✗ Docker compose failed${NC}"
    exit 1
fi

# Wait for services to be ready
echo "Waiting for Prometheus..."
sleep 3

echo "Waiting for Grafana..."
sleep 3

# Step 4: Start GVDB server
echo -e "\n${YELLOW}[4/5] Starting GVDB server...${NC}"

# Kill any existing GVDB process
pkill -f gvdb-all-in-one || true
sleep 1

# Start GVDB in background
nohup ./build/bin/gvdb-all-in-one \
    --port 50051 \
    --data-dir /tmp/gvdb \
    --node-id 1 \
    > /tmp/gvdb.log 2>&1 &

GVDB_PID=$!
echo "GVDB started with PID: $GVDB_PID"

# Wait for GVDB to be ready
echo "Waiting for GVDB to start..."
sleep 3

# Check if GVDB is running
if ps -p $GVDB_PID > /dev/null; then
    echo -e "${GREEN}✓ GVDB server started${NC}"
else
    echo -e "${RED}✗ GVDB server failed to start${NC}"
    echo "Check logs: tail -f /tmp/gvdb.log"
    exit 1
fi

# Step 5: Verify everything is working
echo -e "\n${YELLOW}[5/5] Verifying services...${NC}"

# Check GVDB metrics endpoint
if curl -s http://localhost:9090/metrics > /dev/null; then
    echo -e "${GREEN}✓ GVDB metrics endpoint: http://localhost:9090/metrics${NC}"
else
    echo -e "${RED}✗ GVDB metrics endpoint not responding${NC}"
fi

# Check Prometheus
if curl -s http://localhost:9091/-/healthy > /dev/null; then
    echo -e "${GREEN}✓ Prometheus: http://localhost:9091${NC}"
else
    echo -e "${YELLOW}⚠ Prometheus not ready yet (may take a few more seconds)${NC}"
fi

# Check Grafana
if curl -s http://localhost:3000/api/health > /dev/null; then
    echo -e "${GREEN}✓ Grafana: http://localhost:3000${NC}"
else
    echo -e "${YELLOW}⚠ Grafana not ready yet (may take a few more seconds)${NC}"
fi

# Summary
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}✓ All services started successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Service URLs:"
echo "  - GVDB gRPC:    localhost:50051"
echo "  - GVDB Metrics: http://localhost:9090/metrics"
echo "  - Prometheus:   http://localhost:9091"
echo "  - Grafana:      http://localhost:3000 (admin/admin)"
echo ""
echo "Logs:"
echo "  - GVDB:         tail -f /tmp/gvdb.log"
echo "  - Prometheus:   docker logs -f gvdb-prometheus"
echo "  - Grafana:      docker logs -f gvdb-grafana"
echo ""
echo "To import Grafana dashboard:"
echo "  1. Open http://localhost:3000"
echo "  2. Login (admin/admin)"
echo "  3. Configuration → Data Sources → Add Prometheus"
echo "     - URL: http://prometheus:9090"
echo "  4. Create → Import → Upload grafana/gvdb-dashboard.json"
echo ""
echo "To stop everything:"
echo "  ./stop.sh"
echo ""
