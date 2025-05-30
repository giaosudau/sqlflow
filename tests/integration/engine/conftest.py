"""Engine-specific fixtures for integration tests."""

import tempfile
from pathlib import Path
from typing import Any, Dict, Generator

import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine


@pytest.fixture(scope="function")
def memory_engine() -> Generator[DuckDBEngine, None, None]:
    """Create a DuckDBEngine with in-memory database.

    Yields
    ------
        DuckDBEngine configured for in-memory operations
    """
    engine = DuckDBEngine(database_path=":memory:")
    yield engine
    engine.close()


@pytest.fixture(scope="function")
def persistent_engine() -> Generator[DuckDBEngine, None, None]:
    """Create a DuckDBEngine with persistent database.

    Yields
    ------
        DuckDBEngine configured for persistent operations
    """
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
        db_path = tmp_file.name

    engine = DuckDBEngine(database_path=db_path)
    engine.is_persistent = True

    yield engine

    # Cleanup
    engine.close()
    try:
        Path(db_path).unlink(missing_ok=True)
    except (OSError, PermissionError):
        pass  # Handle case where file is locked


@pytest.fixture(scope="function")
def engine_test_data() -> Dict[str, Any]:
    """Create test data for engine testing.

    Returns:
    -------
        Dictionary with various test datasets
    """
    return {
        "simple_data": [
            {"id": 1, "name": "Alice", "value": 100.0},
            {"id": 2, "name": "Bob", "value": 200.0},
            {"id": 3, "name": "Charlie", "value": 150.0},
        ],
        "large_data": [
            {"id": i, "value": i * 10.5, "category": f"cat_{i % 3}"}
            for i in range(1, 1001)
        ],
        "mixed_types": [
            {"str_col": "test", "int_col": 42, "float_col": 3.14, "bool_col": True},
            {"str_col": "data", "int_col": 100, "float_col": 2.71, "bool_col": False},
        ],
    }


@pytest.fixture(scope="function")
def temp_engine_project() -> Generator[Dict[str, Any], None, None]:
    """Create a temporary project for engine testing.

    Yields
    ------
        Dictionary with project structure for engine tests
    """
    with tempfile.TemporaryDirectory(prefix="sqlflow_engine_test_") as tmp_dir:
        project_paths = {
            "project_dir": tmp_dir,
            "profiles_dir": Path(tmp_dir) / "profiles",
            "data_dir": Path(tmp_dir) / "data",
        }

        # Create directories
        for path in project_paths.values():
            if isinstance(path, Path):
                path.mkdir(exist_ok=True)

        # Create basic profile
        profile_content = """
dev:
  engines:
    duckdb:
      mode: memory
      memory_limit: 256MB
  variables:
    test_env: development
"""
        profile_file = project_paths["profiles_dir"] / "dev.yml"
        profile_file.write_text(profile_content)

        yield project_paths
