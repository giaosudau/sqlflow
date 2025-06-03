# Integration Testing with External Services

This guide covers how to write and run integration tests that depend on external services like PostgreSQL, MinIO, and Redis in SQLFlow.

## Overview

SQLFlow uses a comprehensive integration testing strategy that includes:

- **Local Integration Tests**: Tests that run without external dependencies
- **External Service Tests**: Tests that require real services (PostgreSQL, MinIO, Redis)
- **Mock-based Tests**: Tests that use mocks for isolation
- **End-to-end Tests**: Complete pipeline tests with real data

## External Service Testing Strategy

### Service Dependencies

SQLFlow's integration tests use these external services:

| Service | Port | Purpose | Configuration |
|---------|------|---------|---------------|
| PostgreSQL | 5432 | Database connector testing | `postgres/postgres@localhost:5432` |
| MinIO | 9000-9001 | S3-compatible storage testing | `minioadmin/minioadmin@localhost:9000` |
| Redis | 6379 | Caching and state management | `localhost:6379` (no auth) |

### Test Markers

Use pytest markers to categorize tests:

```python
import pytest

# Mark tests that require external services
@pytest.mark.external_services
def test_postgres_real_connection():
    """Test that requires a real PostgreSQL instance."""
    pass

# Mark tests for specific services
@pytest.mark.postgres
@pytest.mark.external_services
def test_postgres_incremental_loading():
    """Test PostgreSQL incremental loading with real database."""
    pass

@pytest.mark.s3
@pytest.mark.external_services
def test_s3_resilience_patterns():
    """Test S3 connector resilience with real MinIO."""
    pass
```

### Running External Service Tests

#### Using the Integration Test Runner

The recommended way to run external service tests is using the root-level script:

```bash
# Run all external service tests
./run_integration_tests.sh

# Run with verbose output
./run_integration_tests.sh -v

# Run only PostgreSQL tests
./run_integration_tests.sh -k postgres

# Run specific test file
./run_integration_tests.sh -f test_postgres_resilience.py

# Quick smoke tests only
./run_integration_tests.sh --quick

# Keep services running for debugging
./run_integration_tests.sh --keep-services

# Run with coverage reporting
./run_integration_tests.sh --coverage
```

#### Manual Service Management

For development and debugging, you can manage services manually:

```bash
# Start services manually
cd examples/phase2_integration_demo
docker-compose up -d postgres minio redis

# Wait for services to be ready
./scripts/ci_utils.sh  # Has health check functions

# Run tests manually
INTEGRATION_TESTS=true python -m pytest tests/integration/connectors/ -m external_services -v

# Stop services
docker-compose down
```

#### Environment Variables

Set these environment variables for external service tests:

```bash
export INTEGRATION_TESTS=true           # Required for external tests
export LOG_LEVEL=DEBUG                  # Optional: detailed logging
export PYTEST_WORKERS=auto              # Optional: parallel execution
export TEST_POSTGRES_HOST=localhost     # Optional: custom host
export TEST_POSTGRES_PORT=5432          # Optional: custom port
export TEST_MINIO_ENDPOINT=localhost:9000  # Optional: custom MinIO endpoint
```

## Writing External Service Tests

### Test Structure

Follow this pattern for external service tests:

```python
import pytest
from sqlflow.connectors.postgres_connector import PostgresConnector
from sqlflow.core.exceptions import ConnectorError

@pytest.mark.external_services
@pytest.mark.postgres
class TestPostgresConnectorIntegration:
    """Integration tests for PostgreSQL connector with real database."""
    
    @pytest.fixture
    def postgres_config(self):
        """Provide configuration for real PostgreSQL instance."""
        return {
            "host": "localhost",
            "port": 5432,
            "database": "postgres", 
            "username": "postgres",
            "password": "postgres",
        }
    
    @pytest.fixture
    def setup_test_data(self, postgres_config):
        """Set up test data in real database."""
        connector = PostgresConnector()
        connector.configure(postgres_config)
        
        # Create test tables and data
        # ... setup code ...
        
        yield  # Run test
        
        # Cleanup test data
        # ... cleanup code ...
    
    def test_real_connection(self, postgres_config):
        """Test connection to real PostgreSQL instance."""
        connector = PostgresConnector()
        connector.configure(postgres_config)
        
        result = connector.test_connection()
        assert result.success, f"Connection failed: {result.message}"
    
    def test_incremental_loading(self, postgres_config, setup_test_data):
        """Test incremental loading with real data."""
        connector = PostgresConnector()
        connector.configure({
            **postgres_config,
            "sync_mode": "incremental",
            "cursor_field": "updated_at"
        })
        
        # Test incremental read logic
        data = connector.read_incremental(
            object_name="test_table",
            cursor_field="updated_at",
            cursor_value=None  # Initial load
        )
        
        assert len(data) > 0, "Should read initial data"
        # ... additional assertions ...
```

### Service Availability Checking

Always check service availability before running tests:

```python
import socket
import pytest

def is_service_available(host: str, port: int) -> bool:
    """Check if a service is available on the given host:port."""
    try:
        with socket.create_connection((host, port), timeout=5):
            return True
    except (socket.error, socket.timeout):
        return False

@pytest.fixture(scope="session", autouse=True)
def check_external_services():
    """Skip external service tests if services are not available."""
    if not is_service_available("localhost", 5432):
        pytest.skip("PostgreSQL not available at localhost:5432")
    
    if not is_service_available("localhost", 9000):
        pytest.skip("MinIO not available at localhost:9000")
    
    if not is_service_available("localhost", 6379):
        pytest.skip("Redis not available at localhost:6379")
```

### Error Handling and Resilience Testing

Test error scenarios and resilience patterns:

```python
@pytest.mark.external_services
def test_connection_retry_behavior(self, postgres_config):
    """Test that connectors retry failed operations appropriately."""
    # Configure with invalid port to test retry logic
    invalid_config = {**postgres_config, "port": 9999}
    
    connector = PostgresConnector()
    connector.configure(invalid_config)
    
    # Should retry and eventually raise exception
    with pytest.raises(ConnectorError) as exc_info:
        connector.test_connection()
    
    # Verify retry attempts were made
    assert "connection refused" in str(exc_info.value).lower()

@pytest.mark.external_services 
def test_rate_limiting_compliance(self, s3_config):
    """Test that S3 connector respects rate limits."""
    connector = S3Connector()
    connector.configure({
        **s3_config,
        "rate_limit_requests_per_minute": 10  # Low limit for testing
    })
    
    # Make multiple requests rapidly
    start_time = time.time()
    for i in range(15):
        connector.list_objects("test-bucket")
    end_time = time.time()
    
    # Should take at least 30 seconds due to rate limiting
    assert end_time - start_time >= 30, "Rate limiting not enforced"
```

### Data Validation

Validate that real services return expected data:

```python
@pytest.mark.external_services
def test_real_data_integrity(self, setup_test_data, postgres_config):
    """Test that real data is read and written correctly."""
    connector = PostgresConnector()
    connector.configure(postgres_config)
    
    # Read data from real database
    data = connector.read("test_table")
    
    # Validate data structure
    expected_columns = ["id", "name", "created_at", "updated_at"]
    assert all(col in data.columns for col in expected_columns)
    
    # Validate data types
    assert data["id"].dtype == "int64"
    assert data["created_at"].dtype == "datetime64[ns]"
    
    # Validate data content
    assert len(data) > 0, "Should have test data"
    assert data["id"].is_unique, "IDs should be unique"
```

## Service Configuration

### Docker Compose Configuration

The external services are defined in `examples/phase2_integration_demo/docker-compose.yml`:

```yaml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: postgres
      POSTGRES_USER: postgres  
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5
```

### Service Health Checks

The `ci_utils.sh` script provides health check functions:

```bash
# Check if all services are healthy
wait_for_all_services 120  # Wait up to 2 minutes

# Check individual services
wait_for_postgres 60
wait_for_minio 60
wait_for_sqlflow 60

# Test service connectivity
test_service_connectivity
```

## Continuous Integration

### GitHub Actions Integration

The CI workflow runs external service tests:

```yaml
- name: Run integration tests with real services
  env:
    INTEGRATION_TESTS: true
    CI: true
  run: |
    pytest tests/integration/ -k "external_services" -v --tb=short
```

### Local Development

For local development with external services:

```bash
# Start services in background
cd examples/phase2_integration_demo
docker-compose up -d

# Run tests while developing
INTEGRATION_TESTS=true python -m pytest tests/integration/connectors/test_postgres_resilience.py -v

# Stop services when done
docker-compose down
```

## Best Practices

### Test Isolation

1. **Use fresh data**: Create and cleanup test data for each test
2. **Avoid shared state**: Don't depend on data from other tests
3. **Use transactions**: Wrap test data changes in transactions when possible

### Performance Considerations

1. **Minimize data**: Use small datasets for faster test execution
2. **Parallel execution**: Mark tests appropriately for parallel running
3. **Service reuse**: Share service instances across tests in the same session

### Error Handling

1. **Graceful degradation**: Skip tests if services are unavailable
2. **Clear error messages**: Provide helpful failure messages
3. **Cleanup on failure**: Ensure resources are cleaned up even if tests fail

### Documentation

1. **Document service requirements**: Clearly state what services tests need
2. **Provide setup instructions**: Include clear setup steps in test docstrings
3. **Add troubleshooting**: Include common issues and solutions

## Troubleshooting

### Common Issues

**Services not starting:**
```bash
# Check Docker status
docker ps

# Check service logs
docker-compose logs postgres
docker-compose logs minio
docker-compose logs redis

# Restart services
docker-compose restart
```

**Connection failures:**
```bash
# Test connectivity manually
nc -z localhost 5432  # PostgreSQL
nc -z localhost 9000  # MinIO
nc -z localhost 6379  # Redis

# Check if ports are in use
lsof -i :5432
lsof -i :9000
lsof -i :6379
```

**Test failures:**
```bash
# Run tests with more verbose output
./run_integration_tests.sh -v

# Run specific failing test
./run_integration_tests.sh -f test_postgres_resilience.py -v

# Keep services running for debugging
./run_integration_tests.sh --keep-services

# Check test logs
tail -f logs/integration_tests/pytest_output.log
```

**Permission issues:**
```bash
# Ensure scripts are executable
chmod +x run_integration_tests.sh
chmod +x examples/phase2_integration_demo/scripts/ci_utils.sh

# Check Docker permissions
groups $USER  # Should include 'docker' group
```

## Related Resources

- [Resilience Patterns Implementation](../technical/resilience_patterns_spec.md)
- [PostgreSQL Connector Specification](../technical/postgres_connector_spec.md)
- [S3 Connector Specification](../technical/s3_connector_spec.md)
- [Testing Standards](.cursor/rules/04_testing_standards.mdc)
- [CI Utilities](examples/phase2_integration_demo/scripts/ci_utils.sh) 