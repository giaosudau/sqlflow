---
applyTo: 'tests/*.py'
---

# SQLFlow Test File Guidelines

## General Principles
- Follow Test-Driven Development: write tests before or alongside implementation.
- Focus tests on observable behaviors, not internal implementation details.
- Use real implementations and data where possible; minimize mocking.
- Treat tests as documentation and usage examples.

## Test File Organization
- Place unit tests in `tests/unit/`, integration tests in `tests/integration/`, and functional tests in `tests/functional/`.
- Mirror the source directory structure in test directories.
- Name test files with the `test_` prefix for pytest discovery.
- Each test file should focus on a specific component or behavior.

## Test Structure
- Use the Arrange-Act-Assert (AAA) pattern in each test:
    - Arrange: Set up test data, fixtures, and environment.
    - Act: Perform the action under test.
    - Assert: Check expected outcomes.
- Write descriptive test function names that clearly state the scenario and expected result.
- Test both positive and negative/error scenarios.
- Use parameterized tests for multiple cases and edge conditions, with descriptive IDs.

## Fixtures and Setup
- Use pytest fixtures for reusable setup and teardown.
- Prefer fixtures in `conftest.py` for sharing across files.
- Use appropriate fixture scopes (`function`, `class`, `module`).
- Use temporary resources (e.g., `tempfile.TemporaryDirectory`) and realistic test data.
- Clean up all resources after tests.

## Integration Testing
- For tests requiring external services (e.g., PostgreSQL, MinIO), use the integration test runner (`./run_integration_tests.sh`).
- Mark such tests with appropriate pytest markers (e.g., `@pytest.mark.postgres`).
- Ensure tests wait for service readiness and clean up after execution.
- Prefer real data flows and end-to-end scenarios over mocks.

## Minimal Mocking
- Avoid mocking internal SQLFlow components.
- Only mock external dependencies (e.g., network, third-party APIs) at system boundaries.
- Use real test databases (SQLite for unit, actual DBs for integration) where possible.

## Coverage and Quality
- Aim for high coverage, especially on core logic and error handling.
- Ensure tests run without warnings or resource leaks.
- Treat all test failures as critical and address them promptly.

## Documentation and Examples
- Use clear, descriptive test names and docstrings.
- Document test scenarios, expected inputs/outputs, and assumptions.
- Treat tests as primary usage examples for the codebase.

## Example: Integration Test Structure
