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

#### **CLI Tests**
- `tests/integration/cli/test_conditional_pipeline_validation.py`
  - **Dependencies**: Temporary file systems, subprocess calls to sqlflow CLI
  - **Markers**: None
  - **Note**: Tests conditional pipeline validation via CLI

- `tests/integration/cli/test_pipeline_multiline_json.py`
  - **Dependencies**: Temporary file systems, in-memory DuckDB
  - **Markers**: Several tests skipped pending refactoring
  - **Note**: Tests multiline JSON parameter parsing

- `tests/integration/cli/test_validation_integration.py`
  - **Dependencies**: Temporary file systems, subprocess calls to sqlflow CLI
  - **Markers**: None
  - **Note**: Tests CLI validation commands end-to-end

#### **Core Functionality Tests**
- `tests/integration/core/test_create_or_replace_functionality.py`
  - **Dependencies**: In-memory DuckDB, temporary file systems
  - **Markers**: None
  - **Note**: Tests CREATE OR REPLACE functionality

- `tests/integration/core/test_debugging_infrastructure.py`
  - **Dependencies**: Mock loggers, in-memory DuckDB, file system mocks
  - **Markers**: None
  - **Note**: Tests logging, tracing, and debugging infrastructure

- `tests/integration/core/test_complete_incremental_loading_flow.py`
  - **Dependencies**: In-memory DuckDB, temporary CSV files
  - **Markers**: None
  - **Note**: Tests incremental loading patterns with mocked data

- `tests/integration/core/test_enhanced_source_execution.py`
  - **Dependencies**: In-memory DuckDB, temporary CSV files
  - **Markers**: None
  - **Note**: Tests enhanced source execution with watermarks

- `tests/integration/core/test_incremental_strategies_integration.py`
  - **Dependencies**: In-memory DuckDB
  - **Markers**: None
  - **Note**: Tests incremental loading strategies

- `tests/integration/core/test_logging_tracing_integration.py`
  - **Dependencies**: In-memory DuckDB, mock logging
  - **Markers**: None
  - **Note**: Tests observability and tracing

- `tests/integration/core/test_monitoring_integration.py`
  - **Dependencies**: In-memory DuckDB, mock monitoring
  - **Markers**: None
  - **Note**: Tests monitoring and alerting

- `tests/integration/core/test_partitions_integration.py`
  - **Dependencies**: In-memory DuckDB
  - **Markers**: None
  - **Note**: Tests partition detection and management

- `tests/integration/core/test_performance_integration.py`
  - **Dependencies**: In-memory DuckDB
  - **Markers**: None
  - **Note**: Tests performance optimization

- `tests/integration/core/test_schema_integration.py`
  - **Dependencies**: In-memory DuckDB
  - **Markers**: None
  - **Note**: Tests schema evolution and compatibility

- `tests/integration/core/test_transform_handlers_integration.py`
  - **Dependencies**: In-memory DuckDB
  - **Markers**: None
  - **Note**: Tests transform handlers (replace, append, upsert, incremental)

- `tests/integration/core/test_watermark_integration.py`
  - **Dependencies**: In-memory DuckDB, file system
  - **Markers**: None
  - **Note**: Tests watermark management

#### **Connector Tests (Mock-Based)**
- `tests/integration/connectors/test_connector_integration.py`
  - **Dependencies**: Mock file systems, in-memory data, mocked external APIs
  - **Markers**: PostgreSQL tests skipped (require external service)
  - **Note**: Tests connector framework with mocked I/O

- `tests/integration/connectors/test_industry_standard_parameters.py`
  - **Dependencies**: In-memory DuckDB, temporary CSV files
  - **Markers**: None
  - **Note**: Tests Airbyte-compatible parameter validation

- `tests/integration/connectors/test_variable_substitution.py`
  - **Dependencies**: Temporary file systems, in-memory connectors
  - **Markers**: None
  - **Note**: Tests variable substitution in connector configs

- `tests/integration/connectors/test_shopify_integration.py`
  - **Dependencies**: Mock Shopify API responses
  - **Markers**: None
  - **Note**: Tests Shopify connector with mocked data

#### **Engine Tests**
- `tests/integration/engine/test_duckdb_engine_integration.py`
  - **Dependencies**: In-memory DuckDB, temporary files
  - **Markers**: None
  - **Note**: Tests DuckDB engine functionality

- `tests/integration/engine/test_thread_pool_executor_integration.py`
  - **Dependencies**: In-memory processing
  - **Markers**: None
  - **Note**: Tests thread pool execution

#### **Load Mode Tests**
- `tests/integration/load_modes/test_load_modes.py`
  - **Dependencies**: In-memory DuckDB, temporary CSV files
  - **Markers**: None
  - **Note**: Tests all load modes (replace, append, upsert)

- `tests/integration/load_modes/test_load_modes_regression.py`
  - **Dependencies**: In-memory DuckDB
  - **Markers**: None
  - **Note**: Regression tests for load modes

- `tests/integration/load_modes/test_schema_compatibility.py`
  - **Dependencies**: In-memory DuckDB
  - **Markers**: None
  - **Note**: Tests schema compatibility validation

- `tests/integration/load_modes/test_upsert_key_validation.py`
  - **Dependencies**: In-memory DuckDB
  - **Markers**: None
  - **Note**: Tests upsert key validation

- `tests/integration/load_modes/test_upsert_operation_regression.py`
  - **Dependencies**: In-memory DuckDB
  - **Markers**: None
  - **Note**: Regression tests for upsert operations

#### **Pipeline Tests**
- `tests/integration/pipeline/test_ast_to_dag.py`
  - **Dependencies**: Parser and AST processing only
  - **Markers**: None
  - **Note**: Tests pipeline parsing and DAG generation

- `tests/integration/pipeline/test_cycle_detection.py`
  - **Dependencies**: Graph algorithms only
  - **Markers**: None
  - **Note**: Tests dependency cycle detection

- `tests/integration/pipeline/test_validation_pipeline.py`
  - **Dependencies**: Mock validation contexts
  - **Markers**: None
  - **Note**: Tests pipeline validation logic

- `tests/integration/pipeline/test_planner_integration.py`
  - **Dependencies**: In-memory planning only
  - **Markers**: None
  - **Note**: Tests execution plan generation

- `tests/integration/pipeline/test_complete_pipeline_flow.py`
  - **Dependencies**: In-memory DuckDB, temporary files
  - **Markers**: None (but has external service subset)
  - **Note**: Tests complete pipeline execution flows

- `tests/integration/pipeline/test_real_integration.py`
  - **Dependencies**: In-memory DuckDB, temporary files
  - **Markers**: Most tests skipped pending refactoring
  - **Note**: Real integration scenarios (currently disabled)

#### **Persistence Tests**
- `tests/integration/persistence/test_database_persistence.py`
  - **Dependencies**: File-based DuckDB, temporary files
  - **Markers**: Most tests skipped pending profile refactoring
  - **Note**: Tests database persistence across sessions

#### **Storage Tests**
- `tests/integration/storage/test_storage_integration.py`
  - **Dependencies**: File-based storage, temporary directories
  - **Markers**: None
  - **Note**: Tests artifact management and state persistence

#### **UDF Tests**
- `tests/integration/udf/test_udf_core_functionality.py`
  - **Dependencies**: In-memory DuckDB, temporary Python files
  - **Markers**: None
  - **Note**: Tests UDF discovery, registration, and execution

- `tests/integration/udf/test_udf_error_handling.py`
  - **Dependencies**: In-memory DuckDB, temporary Python files
  - **Markers**: None
  - **Note**: Tests UDF error handling and validation

- `tests/integration/udf/test_udf_integration.py`
  - **Dependencies**: In-memory DuckDB, temporary project structure
  - **Markers**: None
  - **Note**: Tests end-to-end UDF integration

- `tests/integration/udf/test_udf_performance.py`
  - **Dependencies**: In-memory DuckDB, performance measurement
  - **Markers**: Memory tests skipped in CI
  - **Note**: Tests UDF performance and scalability

- `tests/integration/udf/test_table_udf_sql_execution.py`
  - **Dependencies**: In-memory DuckDB, temporary Python files
  - **Markers**: None
  - **Note**: Tests table UDF SQL execution patterns

### 🔴 External Service Tests (Require Docker Services)

These tests require real external services and run in the integration-test CI job:

#### **PostgreSQL Connector Tests**
- `tests/integration/connectors/test_postgres_resilience.py`
  - **Dependencies**: PostgreSQL database (localhost:5432)
  - **Markers**: `pytestmark = pytest.mark.external_services`
  - **Services Required**: PostgreSQL
  - **Note**: Tests resilience patterns with real database connections

#### **S3/MinIO Connector Tests**
- `tests/integration/connectors/test_enhanced_s3_connector.py`
  - **Dependencies**: MinIO (S3-compatible storage, localhost:9000)
  - **Markers**: `pytestmark = pytest.mark.external_services`
  - **Services Required**: MinIO
  - **Note**: Tests real S3 operations, cost management, partitioning

- `tests/integration/connectors/test_s3_resilience.py`
  - **Dependencies**: MinIO (S3-compatible storage, localhost:9000)
  - **Markers**: `pytestmark = pytest.mark.external_services`
  - **Services Required**: MinIO
  - **Note**: Tests resilience patterns with real S3 operations

#### **Multi-Service Resilience Tests**
- `tests/integration/connectors/test_resilience_patterns.py`
  - **Dependencies**: PostgreSQL + MinIO for testing patterns across services
  - **Markers**: `pytestmark = pytest.mark.external_services`
  - **Services Required**: PostgreSQL, MinIO
  - **Note**: Tests resilience patterns across multiple service types

#### **Connector Engine Tests (External Service Subset)**
- `tests/integration/core/test_connector_engine_integration.py`
  - **Dependencies**: PostgreSQL + MinIO for connector registration tests
  - **Markers**: `@pytest.mark.external_services` on specific test methods
  - **Services Required**: PostgreSQL, MinIO
  - **Note**: Tests connector engine methods with real services

- `tests/integration/core/test_connector_parameter_validation.py`
  - **Dependencies**: PostgreSQL + MinIO for validation tests
  - **Markers**: `@pytest.mark.external_services` on specific test methods
  - **Services Required**: PostgreSQL, MinIO
  - **Note**: Tests parameter validation with real service connections

## 🏗️ Implementation Status

### ✅ Completed
- [x] **Phase 2 Integration Demo**: Complete end-to-end testing with Docker services
- [x] **External Service Markers**: Tests properly marked with `pytestmark = pytest.mark.external_services`
- [x] **Integration Test Runner**: `run_integration_tests.sh` manages Docker services and runs both demo + tests
- [x] **Local Test Suite**: Comprehensive local tests using in-memory databases and mocks
- [x] **CI Integration**: Both local and external service tests run in CI pipeline
- [x] **Service Management**: Docker Compose integration via Phase 2 demo system
- [x] **Resilience Testing**: Comprehensive resilience pattern testing with real services

### 🚧 Partially Complete
- [x] **CLI Tests**: ✅ Core functionality complete, some tests skipped pending refactoring
- [x] **Core Tests**: ✅ Most functionality complete, comprehensive coverage
- [x] **Connector Tests**: ✅ External service tests complete, some mock tests need updates
- [x] **UDF Tests**: ✅ Complete with comprehensive error handling and performance tests
- [x] **Load Mode Tests**: ✅ Complete with regression testing
- [x] **Pipeline Tests**: ✅ Core functionality complete, some integration tests disabled
- [x] **Storage Tests**: ✅ Complete with artifact management and persistence
- [x] **Engine Tests**: ✅ Complete with DuckDB integration and thread pool testing

### 📋 Needs Attention
- [ ] **Persistence Tests**: Most tests skipped - profile configuration needs stabilization
- [ ] **Real Integration Tests**: Several tests in `test_real_integration.py` skipped - needs refactoring
- [ ] **Performance Tests**: Memory tests skipped in CI - need CI-friendly alternatives

## 🔧 CI Pipeline Integration

### Main CI Job (`test`)
```bash
# Fast feedback - no external services required
pytest tests/unit/ --cov=sqlflow
pytest tests/integration/ -k "not external_services"
./run_all_examples.sh
```

### Integration CI Job (`integration-test`)
```bash
# Comprehensive testing with real services
./run_integration_tests.sh

# This automatically:
# 1. Starts Docker services (PostgreSQL, MinIO, Redis, pgAdmin)
# 2. Runs Phase 2 integration demo (6 pipeline implementations)
# 3. Runs external service integration tests
# 4. Cleans up services
```

### Test Execution Patterns

#### Default (All External Service Tests)
```bash
./run_integration_tests.sh
# Runs: pytest tests/integration/connectors/ -m external_services
```

#### Quick Smoke Test
```bash
./run_integration_tests.sh --quick
# Runs: Single PostgreSQL connection test for fast verification
```

#### Specific Test File
```bash
./run_integration_tests.sh -f test_postgres_resilience.py
# Runs: Specific external service test file
```

#### With Coverage
```bash
./run_integration_tests.sh --coverage
# Adds: --cov=sqlflow --cov-report=html:htmlcov --cov-report=term
```

## 📊 Test Coverage Strategy

| Test Category | Count | External Deps | Speed | CI Frequency |
|---------------|-------|---------------|-------|--------------|
| **CLI Tests** | 3 files | None | Fast (<1min) | Every commit |
| **Core Tests** | 10 files | None | Medium (2-3min) | Every commit |
| **Connector Tests (Local)** | 4 files | None | Fast (<1min) | Every commit |
| **Connector Tests (External)** | 4 files | PostgreSQL + MinIO | Medium (3-5min) | Every commit |
| **Engine Tests** | 2 files | None | Fast (<1min) | Every commit |
| **Load Mode Tests** | 5 files | None | Fast (<1min) | Every commit |
| **Pipeline Tests** | 6 files | None | Medium (2-3min) | Every commit |
| **Storage Tests** | 1 file | None | Fast (<1min) | Every commit |
| **UDF Tests** | 5 files | None | Medium (2-3min) | Every commit |
| **Persistence Tests** | 1 file | File system | Fast (<1min) | Every commit |
| **Phase 2 Demo** | Complete E2E | Full stack | Slow (5-10min) | Every commit |

**Total**: ~50 integration test files with comprehensive coverage across all SQLFlow components.

## 🚀 Benefits of Current Implementation

### For Developers
- ✅ **Fast Local Testing**: Most tests run without external dependencies
- ✅ **Comprehensive External Testing**: Real service validation when needed
- ✅ **Easy Debugging**: External services can be kept running with `--keep-services`
- ✅ **Flexible Execution**: Choose test granularity (quick, specific files, full suite)

### For CI/CD
- ✅ **Efficient Resource Usage**: External services only when needed
- ✅ **Reliable Results**: Docker Compose provides consistent environment
- ✅ **Parallel Execution**: Local and external tests run independently
- ✅ **Clear Failure Attribution**: Know immediately if issue is local or service-related

### For Quality Assurance
- ✅ **Multi-Level Validation**: Unit, local integration, external service integration
- ✅ **Real-World Testing**: External services test production-like scenarios
- ✅ **Regression Prevention**: Comprehensive test coverage at all levels
- ✅ **Performance Monitoring**: Integration tests track performance metrics

## 📖 References

- [Phase 2 Integration Demo](../../examples/phase2_integration_demo/README.md)
- [Integration Test Runner](../../run_integration_tests.sh)
- [Integration Test README](./README.md)
- [CI Workflow](../../.github/workflows/ci.yml) 