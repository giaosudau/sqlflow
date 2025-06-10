# SQLFlow Integration Tests

> **Focused integration testing with proper separation from comprehensive Phase 2 integration demo**

## 🎯 Purpose & Scope

This directory contains **focused integration tests** that complement the comprehensive **Phase 2 Integration Demo** (`examples/phase2_integration_demo/`). The two testing approaches serve different purposes:

### Integration Tests (this directory)
- ✅ **Focused Testing**: Individual connector features and edge cases
- ✅ **Mock-Friendly**: Uses mocks/stubs where external services aren't critical
- ✅ **Fast Execution**: Minimal external dependencies for quick feedback
- ✅ **Component Testing**: Isolated testing of specific functionality
- ✅ **CI-Optimized**: Runs efficiently in GitHub Actions

### Phase 2 Integration Demo
- 🚀 **End-to-End Testing**: Complete workflows with real services (PostgreSQL, MinIO, pgAdmin)
- 🚀 **Production Simulation**: Full Docker Compose stack testing
- 🚀 **Multi-Connector Workflows**: PostgreSQL → Transform → S3 pipelines
- 🚀 **Performance Validation**: Real-world data volumes and scenarios
- 🚀 **Comprehensive Coverage**: All Phase 2 implementations together

## 🛠️ CI Integration

### Regular CI (`test` job)
```bash
# Runs unit tests and basic examples
pytest tests/unit/ --cov=sqlflow
./run_all_examples.sh
```

### Integration CI (`integration-test` job)
```bash
# Runs both Phase 2 demo AND pytest integration tests
./run_integration_tests.sh

# This automatically:
# 1. Starts real services with Docker Compose
# 2. Runs comprehensive Phase 2 demo (6 pipeline tests)
# 3. Runs focused integration tests with external services
# 4. Cleans up services when done
```

## 📋 Test Categories

### Unit Tests (`tests/unit/`)
- **No external dependencies**
- **Mock everything**: Network calls, file system, external services
- **Fast execution**: Hundreds of tests in seconds
- **Component isolation**: Test individual functions/classes

### Integration Tests (`tests/integration/`)
- **Minimal external dependencies**: Only when absolutely necessary
- **Selective execution**: `@pytest.mark.skipif` for external service requirements
- **Feature integration**: Test how components work together
- **Real implementations**: No mocking of core SQLFlow functionality

### Phase 2 Demo (`examples/phase2_integration_demo/`)
- **Real external services**: PostgreSQL, MinIO, Redis, pgAdmin
- **Complete workflows**: Multi-step data processing pipelines
- **Production patterns**: Industry-standard parameters, resilience, cost management
- **Performance testing**: Real data volumes and concurrent operations

## 🚦 Test Execution Guidelines

### Local Development
```bash
# Run unit tests (no external services needed)
pytest tests/unit/

# Run integration tests (mocked/minimal dependencies)
pytest tests/integration/

# Run integration tests with external services (optional)
INTEGRATION_TESTS=true pytest tests/integration/

# Run comprehensive Phase 2 demo (requires Docker)
cd examples/phase2_integration_demo
python3 run_demo.py
```

### CI/GitHub Actions
```bash
# Unit tests run in every PR/push (fast feedback)
pytest tests/unit/

# Integration tests run with external services on PR/push
./run_integration_tests.sh
```

## 🎯 When to Add Tests Where

### Add to `tests/unit/` when:
- Testing individual functions or classes
- Mocking external dependencies is straightforward
- Fast execution is critical
- Testing error conditions and edge cases

### Add to `tests/integration/` when:
- Testing how multiple components work together
- Minimal external dependencies needed
- Testing specific connector features in isolation
- Mock setup would be more complex than real implementation

### Add to Phase 2 Demo when:
- Testing complete end-to-end workflows
- Validating production scenarios
- Testing multiple connectors together
- Performance and scalability testing

## 🔧 Configuration

### Integration Test Skipping
```python
# Skip tests that require external services by default
@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TESTS", "").lower() in ["true", "1"],
    reason="Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
)
def test_external_service_feature():
    # Test requiring external service
    pass
```

### External Service Configuration
```python
# Use localhost configuration for local testing
@pytest.fixture
def service_config():
    return {
        "host": "localhost",
        "port": 5432,
        # ... other config
    }
```

## 📊 Test Coverage Strategy

| Test Type | Coverage | Speed | External Deps | CI Frequency |
|-----------|----------|-------|---------------|--------------|
| **Unit** | High (90%+) | Fast (<1min) | None | Every commit |
| **Integration** | Medium (70%+) | Medium (2-5min) | Minimal | Every commit |
| **Phase 2 Demo** | Complete E2E | Slow (5-10min) | Full stack | Every commit |

## 🚀 Benefits of This Approach

### For Developers
- ✅ **Fast Feedback**: Unit tests provide immediate results
- ✅ **Focused Debugging**: Integration tests isolate specific issues
- ✅ **Real Validation**: Phase 2 demo validates complete functionality
- ✅ **Flexible Testing**: Choose appropriate test level for each scenario

### For CI/CD
- ✅ **Efficient Resource Usage**: Parallel execution of different test types
- ✅ **Reliable Results**: External services in controlled Docker environment
- ✅ **Comprehensive Coverage**: All testing levels covered appropriately
- ✅ **Clear Separation**: Each test type has defined purpose and scope

### For Quality Assurance
- ✅ **Multi-Level Validation**: Component, integration, and system testing
- ✅ **Real-World Scenarios**: Phase 2 demo tests production patterns
- ✅ **Regression Prevention**: Unit tests catch code-level regressions
- ✅ **Performance Monitoring**: Integration demo tracks performance metrics

## 📖 References

- [Phase 2 Integration Demo](../../examples/phase2_integration_demo/README.md)
- [Phase 2 Resilience Patterns](../../examples/phase2_integration_demo/RESILIENCE_DEMO.md)
- [SQLFlow Testing Strategy](../README.md)
- [CI/CD Workflow](../../.github/workflows/ci.yml)

---

**Ready to test SQLFlow comprehensively? Use the right test for the right purpose! 🚀** 