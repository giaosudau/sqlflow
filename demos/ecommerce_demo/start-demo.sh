#!/bin/bash
# Script to start all services for the SQLFlow ecommerce demo

# Exit on error
set -e

echo "🚀 Starting SQLFlow Ecommerce Demo Environment..."

# Get the absolute path of the repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Check that we're running from the right directory
if [[ ! -f "${SCRIPT_DIR}/docker-compose.yml" ]]; then
  echo "❌ Error: This script must be run from the ecommerce_demo directory"
  echo "Please run: cd /path/to/sqlflow/demos/ecommerce_demo && ./start-demo.sh"
  exit 1
fi

# Check Docker is installed and running
if ! command -v docker &> /dev/null; then
  echo "❌ Error: Docker is not installed or not in your PATH"
  echo "Please install Docker and Docker Compose before running this script"
  exit 1
fi

# Check that 'docker compose' is available
if ! docker compose version &> /dev/null; then
  echo "❌ Error: 'docker compose' is not available. Please install Docker Compose v2 (integrated with Docker CLI)."
  exit 1
fi

# Navigate to the repo root
cd "$(dirname "$0")/../.."
ROOT_DIR=$(pwd)

# Ensure we're in the correct directory
if [ ! -f "pyproject.toml" ]; then
    echo "Error: Not in the SQLFlow repository root directory"
    exit 1
fi

# Make sure target directory exists
mkdir -p "${ROOT_DIR}/demos/ecommerce_demo/target"

# Create and activate a virtual environment
echo "Creating virtual environment..."
python -m venv "${ROOT_DIR}/.venv"
source "${ROOT_DIR}/.venv/bin/activate"

# Install dependencies and package
echo "Installing SQLFlow and dependencies..."
pip install --upgrade pip wheel
pip install -e .

# Build SQLFlow wheel
echo "Building wheel..."
python -m build --wheel
WHEEL_PATH=$(find "${ROOT_DIR}/dist" -name "sqlflow-*.whl" | sort -r | head -n 1)

# Copy the wheel to the demo dist directory
mkdir -p "${ROOT_DIR}/demos/ecommerce_demo/dist"
cp "${WHEEL_PATH}" "${ROOT_DIR}/demos/ecommerce_demo/dist/"

# Navigate to the demo directory
cd "${ROOT_DIR}/demos/ecommerce_demo"

# Install Docker Compose if not available
if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose not found. Please install it first."
    exit 1
fi

# Check if SQLFlow Docker image exists, rebuild if not
if ! docker image inspect sqlflow-ecommerce-demo &> /dev/null; then
    echo "Building Docker image..."
    docker build -t sqlflow-ecommerce-demo .
fi

# Stop and remove containers if they exist
docker-compose down

# Recreate and start containers
echo "Starting containers..."
docker-compose up -d

# Wait for containers to be ready
echo "Waiting for containers to be ready..."
sleep 5

# Run tests to verify the fix
echo "Running debug tests..."
docker-compose exec sqlflow python /app/sqlflow/demos/ecommerce_demo/simple_debug.py

echo "Running SQLFlow demo..."
docker-compose exec sqlflow python -m sqlflow run /app/sqlflow/demos/ecommerce_demo/pipelines/simple_test.sf --profile production

echo "Demo is running! Access services at:

- MinIO Console: http://localhost:9001 (minioadmin / minioadmin)
- Mock Server: http://localhost:1080

To run commands inside the SQLFlow container:
  docker-compose exec sqlflow bash

To execute a specific pipeline:
  docker-compose exec sqlflow python -m sqlflow run /app/sqlflow/demos/ecommerce_demo/pipelines/[pipeline-file].sf --profile production

To stop the demo:
  docker-compose down"
