# SQLFlow Integration Test Classification

> **Classification of integration tests by external service dependencies**

## 🎯 Purpose

This document classifies all integration tests into two categories:
1. **Local Tests**: Can run without external services (use mocks/in-memory)
2. **External Service Tests**: Require real external services (PostgreSQL, MinIO, etc.)

This classification helps maintain an efficient CI/CD pipeline where:
- Local tests run in every commit for fast feedback
- External service tests run with Docker services for comprehensive validation

## 📊 Test Classification

### 🟢 Local Tests (No External Services Required)

These tests can run in the main CI job without Docker services:

#### **Core Functionality Tests**
- `tests/integration/core/test_create_or_replace_functionality.py`
  - **Dependencies**: In-memory DuckDB only
  - **Markers**: None needed
  - **Note**: Tests SQL generation and execution patterns

- `tests/integration/core/test_debugging_infrastructure.py`
  - **Dependencies**: Mock loggers, in-memory state
  - **Markers**: None needed
  - **Note**: Tests logging and tracing infrastructure

#### **Pipeline Tests**
- `tests/integration/pipeline/test_ast_to_dag.py`
  - **Dependencies**: Parser and AST only
  - **Markers**: None needed
  - **Note**: Tests pipeline parsing and DAG generation

- `tests/integration/pipeline/test_cycle_detection.py`
  - **Dependencies**: Graph algorithms only
  - **Markers**: None needed
  - **Note**: Tests dependency cycle detection

- `tests/integration/pipeline/test_validation_pipeline.py`
  - **Dependencies**: Mock validation contexts
  - **Markers**: None needed
  - **Note**: Tests pipeline validation logic

- `tests/integration/pipeline/test_planner_integration.py`
  - **Dependencies**: In-memory planning only
  - **Markers**: None needed
  - **Note**: Tests execution plan generation

#### **Connector Tests (Mock-Based)**
- `tests/integration/connectors/test_connector_integration.py`
  - **Dependencies**: Mock file systems, in-memory data
  - **Markers**: None needed
  - **Note**: Tests connector framework with mocked I/O

- `tests/integration/connectors/test_industry_standard_parameters.py`
  - **Dependencies**: In-memory DuckDB, mock CSV files
  - **Markers**: None needed
  - **Note**: Tests parameter validation and processing

- `tests/integration/connectors/test_variable_substitution.py`
  - **Dependencies**: Mock file systems
  - **Markers**: None needed
  - **Note**: Tests variable substitution in connector configs

#### **Load Mode Tests**
- `tests/integration/load_modes/test_*.py`
  - **Dependencies**: In-memory DuckDB only
  - **Markers**: None needed
  - **Note**: All load mode tests use in-memory databases

#### **UDF Tests**
- `tests/integration/udf/test_*.py`
  - **Dependencies**: In-memory DuckDB, file system mocks
  - **Markers**: None needed
  - **Note**: Tests UDF discovery, registration, and execution

#### **Engine Tests**
- `tests/integration/engine/test_duckdb_engine_integration.py`
  - **Dependencies**: In-memory DuckDB only
  - **Markers**: None needed
  - **Note**: Tests engine functionality without external connections

#### **CLI Tests**
- `tests/integration/cli/test_*.py`
  - **Dependencies**: Temporary file systems, in-memory databases
  - **Markers**: None needed
  - **Note**: Tests CLI functionality with mocked environments

### 🔴 External Service Tests (Require Docker Services)

These tests require real external services and should run in the integration-test CI job:

#### **Enhanced S3 Connector Tests**
- `tests/integration/connectors/test_enhanced_s3_connector.py`
  - **Dependencies**: MinIO (S3-compatible storage)
  - **Markers**: `external_services`
  - **Services Required**: MinIO
  - **Note**: Tests real S3 operations, cost management, partitioning

#### **PostgreSQL Connector Tests**
- `tests/integration/connectors/test_postgres_resilience.py`
  - **Dependencies**: PostgreSQL database
  - **Markers**: `external_services`
  - **Services Required**: PostgreSQL
  - **Note**: Tests resilience patterns with real database connections

#### **S3 Resilience Tests**
- `tests/integration/connectors/test_s3_resilience.py`
  - **Dependencies**: MinIO (S3-compatible storage)
  - **Markers**: `external_services`
  - **Services Required**: MinIO
  - **Note**: Tests resilience patterns with real S3 operations

#### **Resilience Pattern Tests**
- `tests/integration/connectors/test_resilience_patterns.py`
  - **Dependencies**: Various external services for testing patterns
  - **Markers**: `external_services`
  - **Services Required**: PostgreSQL, MinIO
  - **Note**: Tests resilience patterns across multiple service types

#### **Complete Pipeline Flow Tests (with External Services)**
- `tests/integration/pipeline/test_complete_pipeline_flow.py` (subset)
  - **Dependencies**: Real connector integrations
  - **Markers**: `external_services` (for specific test methods)
  - **Services Required**: PostgreSQL, MinIO
  - **Note**: Some tests in this file require external services

#### **Incremental Loading Tests (with Real Data)**
- `tests/integration/core/test_complete_incremental_loading_flow.py` (subset)
  - **Dependencies**: Real databases for state persistence
  - **Markers**: `external_services` (for specific test methods)
  - **Services Required**: PostgreSQL
  - **Note**: Tests that verify incremental loading with real databases

## 🏗️ Refactoring Plan

### Phase 1: Mark External Service Tests ✅

Add pytest markers to classify tests:

```python
import pytest

@pytest.mark.external_services
def test_that_requires_external_services():
    """Test that needs real PostgreSQL/MinIO/etc."""
    pass

def test_that_runs_locally():
    """Test that uses mocks/in-memory only."""
    pass
```

### Phase 2: Update Docker Service Tests 

Move external service tests to use Phase 2 integration demo infrastructure:

1. **PostgreSQL Tests**:
   - Use `postgres` service from docker-compose.yml
   - Connect to `localhost:5432` with credentials from demo setup
   - Leverage existing database schema and test data

2. **MinIO Tests**:
   - Use `minio` service from docker-compose.yml  
   - Connect to `localhost:9000` with demo credentials
   - Utilize pre-populated test buckets and data

3. **Multi-Service Tests**:
   - Use complete docker-compose stack
   - Test real end-to-end workflows
   - Verify service interactions

### Phase 3: Optimize CI Pipeline

1. **Main CI Job** (`test`):
   ```bash
   # Fast feedback - no external services
   pytest tests/unit/
   pytest tests/integration/ -k "not external_services"
   ./run_all_examples.sh
   ```

2. **Integration CI Job** (`integration-test`):
   ```bash
   # Comprehensive testing with real services
   cd examples/phase2_integration_demo
   source scripts/ci_utils.sh
   run_complete_integration_test
   pytest tests/integration/ -k "external_services"
   ```

### Phase 4: Pre-commit Optimization

Update `.pre-commit-config.yaml` to run only local tests:

```yaml
- id: pytest-local
  name: pytest (local tests only)
  entry: pytest
  language: python
  args: [
    'tests/unit/',
    'tests/integration/',
    '-k', 'not external_services',
    '--maxfail=3',
    '-q'
  ]
```

## 🔧 Implementation Status

### ✅ Completed
- [x] Created shared CI utilities script
- [x] Updated CI workflow to use shared utilities
- [x] Updated quick_start.sh to use shared utilities
- [x] Fixed failing unit tests

### 🚧 In Progress
- [ ] Add `external_services` markers to tests
- [ ] Update external service tests to use docker-compose services
- [ ] Update pre-commit configuration

### 📋 Planned
- [ ] Create integration test environment setup scripts
- [ ] Add performance benchmarks for external service tests
- [ ] Create documentation for adding new integration tests

## 📈 Benefits

### For Developers
- **Fast Feedback**: Local tests run in <2 minutes
- **Comprehensive Coverage**: External service tests validate real scenarios
- **Easy Debugging**: Clear separation between test types
- **Consistent Environment**: Shared utilities ensure consistent behavior

### For CI/CD
- **Efficient Resource Usage**: Only spin up Docker services when needed
- **Parallel Execution**: Local and external tests can run independently
- **Reliable Results**: External services in controlled Docker environment
- **Clear Failure Attribution**: Know immediately if issue is local or service-related

### For Quality Assurance
- **Multi-Level Validation**: Unit, integration (local), integration (external)
- **Real-World Testing**: External services test production-like scenarios
- **Regression Prevention**: Comprehensive test coverage at all levels
- **Performance Monitoring**: External service tests can track performance metrics

## 📖 References

- [Phase 2 Integration Demo](../examples/phase2_integration_demo/README.md)
- [CI Utilities Script](../examples/phase2_integration_demo/scripts/ci_utils.sh)
- [CI Workflow](../.github/workflows/ci.yml)
- [Pre-commit Configuration](../.pre-commit-config.yaml) 