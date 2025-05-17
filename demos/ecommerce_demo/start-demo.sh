#!/bin/bash
# Script to start all services for the SQLFlow ecommerce demo

# Exit on error
set -e

# Function to print colored text
print_colored() {
  local color=$1
  local text=$2
  
  case $color in
    "blue") echo -e "\033[1;34m$text\033[0m" ;;
    "green") echo -e "\033[1;32m$text\033[0m" ;;
    "red") echo -e "\033[1;31m$text\033[0m" ;;
    "yellow") echo -e "\033[1;33m$text\033[0m" ;;
    *) echo "$text" ;;
  esac
}

print_colored "blue" "🚀 Starting SQLFlow Ecommerce Demo Environment..."
print_colored "blue" "================================================="
echo

# Get the absolute path of the repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Check that we're running from the right directory
if [[ ! -f "${SCRIPT_DIR}/docker-compose.yml" ]]; then
  print_colored "red" "❌ Error: This script must be run from the ecommerce_demo directory"
  print_colored "yellow" "Please run: cd /path/to/sqlflow/demos/ecommerce_demo && ./start-demo.sh"
  exit 1
fi

# Check Docker is installed and running
if ! command -v docker &> /dev/null; then
  print_colored "red" "❌ Error: Docker is not installed or not in your PATH"
  print_colored "yellow" "Please install Docker and Docker Compose before running this script"
  exit 1
fi

# Check that 'docker compose' is available
if ! docker compose version &> /dev/null; then
  print_colored "red" "❌ Error: 'docker compose' is not available. Please install Docker Compose v2 (integrated with Docker CLI)."
  exit 1
fi

# Make sure target and dist directories exist
mkdir -p "${SCRIPT_DIR}/target"
mkdir -p "${SCRIPT_DIR}/dist"

# Build SQLFlow package
print_colored "blue" "📦 Building SQLFlow Package"
print_colored "blue" "=========================="

# Navigate to the repo root
cd "$REPO_ROOT"

# Set up Python venv if it doesn't exist
if [ ! -d ".venv" ]; then
  print_colored "yellow" "Creating virtual environment..."
  python -m venv .venv
fi

# Activate the virtual environment
print_colored "yellow" "Activating virtual environment..."
source .venv/bin/activate

# Install dev dependencies and build tools
print_colored "yellow" "Installing build dependencies..."
pip install --upgrade pip wheel build

# Install package in development mode
print_colored "yellow" "Installing SQLFlow in development mode..."
pip install -e .

# Build the wheel package
print_colored "yellow" "Building wheel package..."
python -m build --wheel

# Find the newest wheel file
WHEEL_PATH=$(find "${REPO_ROOT}/dist" -name "sqlflow-*.whl" | sort -r | head -n 1)

if [ -z "$WHEEL_PATH" ]; then
  print_colored "red" "❌ Failed to build SQLFlow wheel. Please check for errors."
  exit 1
fi

print_colored "green" "✅ SQLFlow wheel built successfully: $(basename $WHEEL_PATH)"

# Copy the wheel to the demo dist directory
print_colored "yellow" "Copying wheel to demo directory..."
cp "$WHEEL_PATH" "${SCRIPT_DIR}/dist/"
print_colored "green" "✅ Wheel copied successfully"

# Return to the demo directory
cd "${SCRIPT_DIR}"

# Check if SQLFlow Docker image exists and rebuild it
print_colored "yellow" "Building Docker image..."
docker build -t sqlflow-ecommerce-demo .
print_colored "green" "✅ Docker image built successfully"

# Stop and remove containers if they exist
print_colored "yellow" "Stopping any existing containers..."
docker compose down

# Recreate and start containers
print_colored "yellow" "Starting containers..."
docker compose up -d

# Wait for containers to be ready
print_colored "yellow" "Waiting for containers to be ready..."
sleep 10

print_colored "green" "✅ Demo environment is running!"
echo
print_colored "blue" "🔎 Next Steps:"
echo "1. Run './init-demo.sh' to initialize data and run demo pipelines"
echo "2. Or explore manually with these commands:"
echo 
echo "   # Showcase basic CSV processing:"
echo "   docker compose exec sqlflow sqlflow pipeline run /app/sqlflow/demos/ecommerce_demo/pipelines/showcase_01_basic_csv_processing.sf \\"
echo "     --vars '{\"date\": \"$(date '+%Y-%m-%d')\"}'"
echo
echo "   # Showcase multi-connector integration (Postgres, S3, API):"
echo "   docker compose exec sqlflow sqlflow pipeline run /app/sqlflow/demos/ecommerce_demo/pipelines/showcase_02_multi_connector_integration.sf \\"
echo "     --vars '{\"date\": \"$(date '+%Y-%m-%d')\", \"API_TOKEN\": \"your_api_token\"}'"
echo
print_colored "blue" "🌐 Access services at:"
echo "- PostgreSQL: localhost:5432 (User: sqlflow, Password: sqlflow123, DB: ecommerce)"
echo "- MinIO Console: http://localhost:9001 (User: minioadmin, Password: minioadmin)"
echo "- Mock Server: http://localhost:1080"
echo
print_colored "blue" "🔍 To debug or explore:"
echo "- To open a shell in the SQLFlow container:"
echo "  docker compose exec sqlflow bash"
echo
print_colored "blue" "🧹 To stop the demo:"
echo "- docker compose down"
