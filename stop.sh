#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}Stopping GVDB and Monitoring Stack${NC}"
echo -e "${YELLOW}========================================${NC}"

# Stop GVDB server
echo -e "\n${YELLOW}[1/2] Stopping GVDB server...${NC}"
pkill -f gvdb-single-node
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ GVDB server stopped${NC}"
else
    echo -e "${YELLOW}⚠ No GVDB server running${NC}"
fi

# Stop Docker Compose
echo -e "\n${YELLOW}[2/2] Stopping Prometheus + Grafana...${NC}"
cd grafana
docker compose down
cd ..

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Monitoring stack stopped${NC}"
else
    echo -e "${RED}✗ Failed to stop Docker containers${NC}"
fi

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}✓ All services stopped${NC}"
echo -e "${GREEN}========================================${NC}"
