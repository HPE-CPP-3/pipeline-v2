#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== Pipeline V2 Local Development Setup ===${NC}"
echo ""

# Check prerequisites
echo -e "${YELLOW}Step 1: Checking prerequisites...${NC}"

if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker is not installed.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker is installed${NC}"

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo -e "${RED}✗ Docker Compose is not installed.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker Compose is available${NC}"

echo ""
echo -e "${YELLOW}Step 2: Starting local infrastructure (Redis + InfluxDB)...${NC}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.dev.yaml"

if [ ! -f "$COMPOSE_FILE" ]; then
    echo -e "${RED}✗ docker-compose file not found at: ${COMPOSE_FILE}${NC}"
    exit 1
fi

# Start docker-compose
if command -v docker-compose &> /dev/null; then
    docker-compose -f "$COMPOSE_FILE" up -d
else
    docker compose -f "$COMPOSE_FILE" up -d
fi

echo -e "${GREEN}✓ Services started${NC}"

echo ""
echo -e "${YELLOW}Step 3: Waiting for services to be ready...${NC}"

# Wait for Redis
for i in {1..30}; do
    if docker exec pipeline-redis redis-cli ping &> /dev/null; then
        echo -e "${GREEN}✓ Redis is ready${NC}"
        break
    fi
    echo -n "."
    sleep 1
done

# Wait for InfluxDB
for i in {1..30}; do
    if curl -s http://localhost:8086/health &> /dev/null; then
        echo -e "${GREEN}✓ InfluxDB is ready${NC}"
        break
    fi
    echo -n "."
    sleep 1
done

echo ""
echo -e "${YELLOW}Step 4: Setting up InfluxDB bucket and permissions...${NC}"

# Wait a bit more for InfluxDB to fully initialize
sleep 5

# Setup InfluxDB (idempotent)
docker exec pipeline-influxdb influx setup \
  --username admin \
  --password password123 \
  --org pipeline-v2 \
  --bucket metrics \
  --retention 30d \
  --token dev-token-change-me \
  --force 2>/dev/null || true

echo -e "${GREEN}✓ InfluxDB configured${NC}"

echo ""

echo -e "${YELLOW}Step 5: Setting up Python venv + installing dependencies...${NC}"

cd "$ROOT_DIR"

if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    echo -e "${GREEN}✓ Created venv at .venv${NC}"
fi

# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install --upgrade pip

if [ -f "scripts/requirements.txt" ]; then
    pip install -r scripts/requirements.txt
fi

pip install -e .

# macOS fix: Python skips hidden .pth files, which breaks editable installs.
# Some environments end up with __editable__*.pth marked as hidden.
if [ "$(uname -s)" = "Darwin" ] && command -v chflags &> /dev/null; then
    for pth in .venv/lib/python*/site-packages/__editable__*.pth; do
        [ -f "$pth" ] || continue
        chflags nohidden "$pth" 2>/dev/null || true
    done
fi

echo -e "${GREEN}✓ Dependencies installed${NC}"

echo ""
echo -e "${GREEN}=== Local Setup Complete! ===${NC}"
echo ""
echo -e "${YELLOW}Service URLs:${NC}"
echo -e "  Redis:     ${GREEN}localhost:6380${NC}"
echo -e "  InfluxDB:  ${GREEN}http://localhost:8086${NC}"
echo ""
echo -e "${YELLOW}InfluxDB credentials:${NC}"
echo -e "  URL:      ${GREEN}http://localhost:8086${NC}"
echo -e "  Org:      ${GREEN}pipeline-v2${NC}"
echo -e "  Bucket:   ${GREEN}metrics${NC}"
echo -e "  Token:    ${GREEN}dev-token-change-me${NC}"
echo -e "  Username: ${GREEN}admin${NC}"
echo -e "  Password: ${GREEN}password123${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Set up a Prometheus endpoint (see docs/CLUSTER_SETUP.md)"
echo "  2. Copy configs/.env.example to .env and configure:"
echo "     export PIPELINE_PROMETHEUS_URL=http://<your-prometheus>:9090"
echo "     export PIPELINE_REDIS_HOST=localhost"
echo "     export PIPELINE_REDIS_PORT=6380"
echo "  3. Test the pipeline:"
echo "     pipeline-agentic --pod <pod-name> --namespace <namespace>"
echo ""
echo -e "${YELLOW}To stop services:${NC}"
echo "  docker-compose -f scripts/docker-compose.dev.yaml down"
echo ""
