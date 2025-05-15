#!/usr/bin/env bash
# Script to initialize and test the SQLFlow ecommerce demo environment

# Check that we're running from the right directory
if [[ ! -f "docker-compose.yml" ]]; then
  echo "❌ Error: This script must be run from the ecommerce_demo directory"
  echo "Please run: cd /path/to/sqlflow/demos/ecommerce_demo && ./init-demo.sh"
  exit 1
fi

# Set environment variables
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export API_TOKEN=demo-token
export DATE=$(date '+%Y-%m-%d')

echo "📊 SQLFlow Ecommerce Demo Initialization"
echo "========================================"
echo

# Check if services are up
echo "🔍 Checking services..."
if ! docker compose ps | grep -q "sqlflow-postgres.*Up"; then
  echo "❌ PostgreSQL is not running. Please start the services with 'docker compose up -d'"
  exit 1
fi

if ! docker compose ps | grep -q "sqlflow-minio.*Up"; then
  echo "❌ MinIO is not running. Please start the services with 'docker compose up -d'"
  exit 1
fi

if ! docker compose ps | grep -q "sqlflow-mockserver.*Up"; then
  echo "❌ MockServer is not running. Please start the services with 'docker compose up -d'"
  exit 1
fi

echo "✅ All services are running"
echo

# Test PostgreSQL connection
echo "🔍 Testing PostgreSQL connection..."
if ! docker compose exec -T postgres psql -U sqlflow -d ecommerce -c "SELECT 'Connection successful';" > /dev/null 2>&1; then
  echo "❌ Failed to connect to PostgreSQL"
  exit 1
fi
echo "✅ PostgreSQL connection successful"
echo

# Test MinIO connection
echo "🔍 Testing MinIO connection..."
if ! curl -s http://localhost:9000/minio/health/live > /dev/null; then
  echo "❌ Failed to connect to MinIO"
  exit 1
fi
echo "✅ MinIO connection successful"
echo

# Test MockServer connection
echo "🔍 Testing MockServer connection..."
if ! curl -s http://localhost:1080/status > /dev/null; then
  echo "❌ Failed to connect to MockServer"
  exit 1
fi
echo "✅ MockServer connection successful"
echo

# Run SQLFlow pipeline inside the Docker container
echo "📈 Running SQLFlow pipeline..."
docker compose exec sqlflow sqlflow pipeline run /app/sqlflow/demos/ecommerce_demo/pipelines/daily_sales_report_docker.sf --vars "{\"date\": \"$DATE\", \"API_TOKEN\": \"$API_TOKEN\"}"
echo
echo "✅ Demo initialization complete!"
echo
echo "🔗 Access services:"
echo "   - PostgreSQL: localhost:5432 (User: sqlflow, Password: sqlflow123, DB: ecommerce)"
echo "   - MinIO Console: http://localhost:9001 (User: minioadmin, Password: minioadmin)"
echo "   - MockServer: http://localhost:1080"
echo
echo "📊 To run the pipeline again:"
echo "   docker compose exec sqlflow sqlflow pipeline run /app/sqlflow/demos/ecommerce_demo/pipelines/daily_sales_report_docker.sf --vars '{\"date\": \"$DATE\", \"API_TOKEN\": \"$API_TOKEN\"}'"
