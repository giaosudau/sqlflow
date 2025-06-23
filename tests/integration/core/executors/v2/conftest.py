"""Pytest configuration and fixtures for integration tests.

This module provides fixtures for managing test resources and dependencies.
It follows SQLFlow's testing standards for proper test isolation and resource management.
"""

import os
import tempfile
from collections.abc import Generator

import pytest

from sqlflow.connectors.registry.enhanced_registry import enhanced_registry
from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.executors.v2 import ExecutionCoordinator
from sqlflow.core.executors.v2.execution.context import create_test_context


@pytest.fixture(scope="function")
def temp_db_path() -> Generator[str, None, None]:
    """Create a temporary database file that gets cleaned up after the test.

    Yields:
        str: Path to the temporary database file.
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_path = tmp_file.name

    try:
        yield db_path
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


@pytest.fixture(scope="function")
def db_engine() -> DuckDBEngine:
    """Create a DuckDBEngine instance for testing using an in-memory database.

    Returns:
        DuckDBEngine: An initialized DuckDB engine.
    """
    # Each test gets an isolated, in-memory database
    return DuckDBEngine(":memory:")


@pytest.fixture(scope="function")
def execution_context(db_engine: DuckDBEngine):
    """Create an execution context with the test database engine.

    Args:
        db_engine: The database engine fixture.

    Returns:
        ExecutionContext: An initialized execution context.
    """
    return create_test_context(engine=db_engine, connector_registry=enhanced_registry)


@pytest.fixture(scope="function")
def coordinator() -> ExecutionCoordinator:
    """Create an ExecutionCoordinator instance for testing.

    Returns:
        ExecutionCoordinator: A new ExecutionCoordinator instance.
    """
    return ExecutionCoordinator()


@pytest.fixture(scope="function")
def temp_csv_file() -> Generator[str, None, None]:
    """Create a temporary CSV file for testing.

    Yields:
        str: Path to the temporary CSV file.
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n")
        csv_path = f.name

    try:
        yield csv_path
    finally:
        if os.path.exists(csv_path):
            os.unlink(csv_path)


@pytest.fixture(scope="function")
def temp_output_file() -> Generator[str, None, None]:
    """Create a temporary output file for testing exports.

    Yields:
        str: Path to the temporary output file.
    """
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        output_path = f.name

    try:
        yield output_path
    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)
