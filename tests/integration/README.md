# Integration Tests Organization

This directory contains integration tests organized by functional areas to improve maintainability and test discovery.

## Structure Overview

```
tests/integration/
├── conftest.py                 # Central shared fixtures
├── udf/                        # User-Defined Function tests
│   ├── conftest.py            # UDF-specific fixtures
│   ├── test_table_udf_edge_cases.py
│   ├── test_table_udf_performance.py
│   ├── test_udf_data_types.py
│   ├── test_udf_error_handling.py
│   └── test_udf_parameters.py
├── load_modes/                 # Load mode tests (APPEND, REPLACE, MERGE)
│   ├── conftest.py            # Load mode fixtures
│   ├── test_load_modes.py
│   ├── test_load_modes_regression.py
│   └── test_merge_operation_regression.py
├── pipeline/                   # Pipeline execution tests
│   └── conftest.py            # Pipeline-specific fixtures
├── cli/                        # CLI interface tests
│   └── conftest.py            # CLI-specific fixtures
└── test_connectors/           # Connector-specific tests
    └── (various connector tests)
```

## Testing Standards Followed

The reorganization follows the testing standards defined in `04_testing_standards.mdc`:

### 🎯 Focused Test Organization
- **Component-based structure**: Tests grouped by functional area
- **Shared fixtures**: Common setup in central `conftest.py`
- **Component fixtures**: Specific fixtures in component `conftest.py` files
- **Clear naming**: Test files follow `test_<component>_<aspect>.py` pattern

### 🔧 Fixture Management
- **Function scope**: Default scope for most fixtures to ensure isolation
- **Session scope**: Used only for expensive, read-only setup (sample data)
- **Proper cleanup**: All fixtures include teardown/cleanup logic
- **No side effects**: Tests are independent and can run in any order

### 📊 Performance Testing
- **Realistic targets**: Performance benchmarks based on actual usage patterns
- **Memory efficiency**: Tests include memory usage validation
- **Regression prevention**: Baseline performance measurements
- **Optimization tracking**: Debug capabilities for performance analysis

## Key Improvements Made

### 1. Eliminated Duplication
- **Shared fixtures**: Common test data, engines, and executors
- **Consistent patterns**: Standardized test setup across components
- **Reduced boilerplate**: Central configuration management

### 2. Enhanced Test Isolation
- **Independent tests**: Each test can run standalone
- **Clean environments**: Temporary databases and directories
- **Proper scoping**: Fixtures scoped appropriately for their use case

### 3. Better Error Handling
- **Edge case coverage**: Comprehensive testing of error conditions
- **Graceful failures**: Tests handle missing dependencies appropriately
- **Clear error messages**: Descriptive assertion messages

### 4. Performance Focus
- **Benchmark targets**: Realistic performance expectations
- **Memory monitoring**: Track memory usage in large dataset tests
- **Optimization verification**: Tests for performance improvements

## Usage Guidelines

### Running Tests by Component

```bash
# Run all UDF tests
pytest tests/integration/udf/ -v

# Run all load mode tests
pytest tests/integration/load_modes/ -v

# Run specific test categories
pytest tests/integration/udf/test_table_udf_performance.py -v
```

### Running Tests in Parallel

```bash
# Use pytest-xdist for parallel execution
pytest tests/integration/ -n auto -q --disable-warnings
```

### Adding New Tests

1. **Choose the right directory**: Place tests in the appropriate component directory
2. **Use shared fixtures**: Leverage fixtures from `conftest.py` files
3. **Follow naming conventions**: Use descriptive test and file names
4. **Add component fixtures**: Create component-specific fixtures if needed
5. **Document edge cases**: Include tests for error conditions and edge cases

### Fixture Guidelines

- **Use function scope** for most test fixtures
- **Use session scope** only for expensive, immutable setup
- **Include proper cleanup** in all fixtures
- **Document fixture purpose** with clear docstrings
- **Avoid fixture dependencies** that create complex chains

## Migration from Old Structure

Tests have been moved and refactored as follows:

- `test_table_udf_comprehensive_validation.py` → Split into focused files in `udf/`
- `test_load_modes*.py` → Moved to `load_modes/`
- `test_udf_*.py` → Moved to `udf/`
- Large test files → Split into manageable, focused test files

## Future Enhancements

The reorganized structure supports:

1. **Component-specific CI**: Run only relevant tests for changes
2. **Performance tracking**: Historical performance data collection
3. **Test parallelization**: Better parallel execution with isolated components
4. **Documentation integration**: Test documentation alongside code
5. **Coverage analysis**: Component-specific coverage reporting

## Troubleshooting

### Common Issues

- **Fixture not found**: Check if fixture is in the right `conftest.py`
- **Test isolation**: Ensure tests don't depend on execution order
- **Performance failures**: Check if performance targets are realistic
- **Import errors**: Verify test dependencies are available

### Best Practices

- **Run tests locally** before committing
- **Use descriptive test names** that explain what is being tested
- **Include both positive and negative test cases**
- **Test with realistic data sizes**
- **Document any test-specific setup requirements** 