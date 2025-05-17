#!/usr/bin/env bash
# Script to initialize and test the SQLFlow ecommerce demo environment

set -e  # Exit on error

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

print_colored "blue" "📊 SQLFlow Ecommerce Demo Initialization"
print_colored "blue" "========================================"
echo

# Check that we're running from the right directory
if [[ ! -f "docker-compose.yml" ]]; then
  print_colored "red" "❌ Error: This script must be run from the ecommerce_demo directory"
  print_colored "yellow" "Please run: cd /path/to/sqlflow/demos/ecommerce_demo && ./init-demo.sh"
  exit 1
fi

# Check that all containers are running
print_colored "yellow" "🔍 Checking services..."
if ! docker compose ps | grep -q "sqlflow-demo.*Up"; then
  print_colored "red" "❌ Error: SQLFlow container is not running"
  print_colored "yellow" "Please run: ./start-demo.sh"
  exit 1
fi

if ! docker compose ps | grep -q "sqlflow-postgres.*Up"; then
  print_colored "red" "❌ Error: PostgreSQL container is not running"
  print_colored "yellow" "Please run: ./start-demo.sh"
  exit 1
fi

if ! docker compose ps | grep -q "sqlflow-minio.*Up"; then
  print_colored "red" "❌ Error: MinIO container is not running"
  print_colored "yellow" "Please run: ./start-demo.sh"
  exit 1
fi

if ! docker compose ps | grep -q "sqlflow-mockserver.*Up"; then
  print_colored "red" "❌ Error: MockServer container is not running"
  print_colored "yellow" "Please run: ./start-demo.sh"
  exit 1
fi

print_colored "green" "✅ All services are running"

# Test PostgreSQL connection
print_colored "yellow" "🔍 Testing PostgreSQL connection..."
if ! docker compose exec -T postgres pg_isready -U sqlflow -d ecommerce > /dev/null 2>&1; then
  print_colored "red" "❌ Error: Could not connect to PostgreSQL"
  exit 1
fi
print_colored "green" "✅ PostgreSQL connection successful"

# Test MinIO connection
print_colored "yellow" "🔍 Testing MinIO connection..."
if ! docker compose exec -T minio curl -s --head http://localhost:9000/minio/health/live > /dev/null; then
  print_colored "red" "❌ Error: Could not connect to MinIO"
  exit 1
fi
print_colored "green" "✅ MinIO connection successful"

# Set up MinIO bucket if it doesn't exist
print_colored "yellow" "Creating MinIO bucket if it doesn't exist..."
docker compose exec -T minio-init /bin/sh -c "\
  /usr/bin/mc config host add myminio http://minio:9000 minioadmin minioadmin && \
  /usr/bin/mc mb myminio/analytics --ignore-existing && \
  /usr/bin/mc policy set public myminio/analytics" > /dev/null 2>&1
print_colored "green" "✅ MinIO bucket setup complete"

# Test MockServer connection
print_colored "yellow" "🔍 Testing MockServer connection..."
if ! docker compose exec -T mockserver curl -s --head http://localhost:1080/mockserver/status > /dev/null; then
  print_colored "red" "❌ Error: Could not connect to MockServer"
  exit 1
fi
print_colored "green" "✅ MockServer connection successful"

# Set up PostgreSQL tables and data if needed
print_colored "yellow" "Creating PostgreSQL tables and sample data..."
docker compose exec -T postgres psql -U sqlflow -d ecommerce -c "
  -- Check if tables already exist
  DO \$\$
  BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE tablename = 'sales') THEN
      CREATE TABLE sales (
        order_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50),
        product_id VARCHAR(50),
        quantity INTEGER,
        price DECIMAL(10, 2),
        order_date DATE
      );
      
      INSERT INTO sales (order_id, customer_id, product_id, quantity, price, order_date)
      VALUES 
        ('ORD001', 'CUST001', 'PROD001', 2, 19.99, '2023-10-25'),
        ('ORD002', 'CUST002', 'PROD002', 1, 29.99, '2023-10-25'),
        ('ORD003', 'CUST001', 'PROD003', 3, 14.99, '2023-10-25'),
        ('ORD004', 'CUST003', 'PROD002', 2, 29.99, '2023-10-25'),
        ('ORD005', 'CUST004', 'PROD001', 1, 19.99, '2023-10-25');
    END IF;
    
    IF NOT EXISTS (SELECT FROM pg_tables WHERE tablename = 'customers') THEN
      CREATE TABLE customers (
        customer_id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(100),
        region VARCHAR(50)
      );
      
      INSERT INTO customers (customer_id, name, email, region)
      VALUES 
        ('CUST001', 'John Doe', 'john@example.com', 'us-east'),
        ('CUST002', 'Jane Smith', 'jane@example.com', 'us-west'),
        ('CUST003', 'Bob Johnson', 'bob@example.com', 'eu-west'),
        ('CUST004', 'Alice Brown', 'alice@example.com', 'us-east');
    END IF;
    
    IF NOT EXISTS (SELECT FROM pg_tables WHERE tablename = 'products') THEN
      CREATE TABLE products (
        product_id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100),
        category VARCHAR(50),
        price DECIMAL(10, 2)
      );
      
      INSERT INTO products (product_id, name, category, price)
      VALUES 
        ('PROD001', 'Widget A', 'Electronics', 19.99),
        ('PROD002', 'Widget B', 'Home Goods', 29.99),
        ('PROD003', 'Widget C', 'Electronics', 14.99);
    END IF;
  END \$\$;
" > /dev/null 2>&1

print_colored "green" "✅ PostgreSQL setup complete"

print_colored "blue" "🚀 Running Demo Pipelines"
print_colored "blue" "======================="
echo

# View the target directory
print_colored "yellow" "Checking target directory for outputs..."
print_colored "green" "$(docker compose exec -T sqlflow ls -la /app/target)"
echo

# Check the logs from our simplified demo
print_colored "yellow" "Viewing demo output:"
print_colored "green" "$(docker compose logs sqlflow)"
echo

print_colored "green" "✅ Demo initialization complete!"
echo
print_colored "blue" "🔎 What's Next:"
echo 
echo "1. The demo has automatically run a simplified test pipeline"
echo "   that loads data and exports it to a CSV file."
echo
echo "2. You can run additional pipelines with these commands:"
echo
echo "   docker compose exec sqlflow python -c \"import pandas as pd; print(pd.read_csv('/app/sqlflow/demos/ecommerce_demo/data/sales.csv'));\""
echo
echo "3. You can explore PostgreSQL data with:"
echo
echo "   docker compose exec postgres psql -U sqlflow -d ecommerce -c \"SELECT * FROM sales LIMIT 5;\""
echo
echo "4. Access the MinIO web interface at: http://localhost:9001"
echo "   (Username: minioadmin, Password: minioadmin)"
echo
echo "5. Access the MockServer UI at: http://localhost:1080/mockserver/dashboard"
echo
