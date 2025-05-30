"""Persistence-specific fixtures for integration tests."""

import tempfile
from pathlib import Path
from typing import Any, Dict, Generator

import pytest

from sqlflow.core.executors.local_executor import LocalExecutor


@pytest.fixture(scope="function")
def temp_persistent_project() -> Generator[Dict[str, Any], None, None]:
    """Create a temporary project with persistent database configuration.

    Yields
    ------
        Dictionary with project paths and database configuration
    """
    with tempfile.TemporaryDirectory(prefix="sqlflow_persistent_test_") as tmp_dir:
        # Create project structure
        project_paths = {
            "project_dir": tmp_dir,
            "profiles_dir": Path(tmp_dir) / "profiles",
            "pipelines_dir": Path(tmp_dir) / "pipelines",
            "data_dir": Path(tmp_dir) / "data",
        }

        # Create directories
        for path in project_paths.values():
            if isinstance(path, Path):
                path.mkdir(exist_ok=True)

        # Create persistent profile with absolute path in proper YAML format
        db_path = project_paths["data_dir"] / "test_persistent.duckdb"
        profile_content = f"""dev:
  engines:
    duckdb:
      mode: persistent
      path: "{db_path.as_posix()}"
      memory_limit: 512MB
  variables:
    test_mode: persistent
"""
        profile_file = project_paths["profiles_dir"] / "dev.yml"
        profile_file.write_text(profile_content)

        project_paths["db_path"] = db_path
        yield project_paths


@pytest.fixture(scope="function")
def persistent_executor(temp_persistent_project: Dict[str, Any]) -> LocalExecutor:
    """Create a LocalExecutor with persistent database configuration.

    Args:
    ----
        temp_persistent_project: Temporary project fixture

    Returns:
    -------
        LocalExecutor configured for persistent database operations
    """
    executor = LocalExecutor(
        project_dir=temp_persistent_project["project_dir"], profile_name="dev"
    )
    return executor


@pytest.fixture(scope="function")
def sample_persistent_data() -> str:
    """Create sample SQL for persistence testing.

    Returns:
    -------
        SQL statements for creating and testing persistent data
    """
    return """
-- Create test table with sample data
CREATE TABLE customers AS
SELECT 
    1 as customer_id, 'Alice Johnson' as name, 'alice@example.com' as email, 'premium' as tier
UNION ALL
SELECT 
    2 as customer_id, 'Bob Smith' as name, 'bob@example.com' as email, 'standard' as tier
UNION ALL
SELECT 
    3 as customer_id, 'Charlie Brown' as name, 'charlie@example.com' as email, 'premium' as tier;

-- Create orders table
CREATE TABLE orders AS
SELECT 
    101 as order_id, 1 as customer_id, 250.00 as amount, '2024-01-15' as order_date
UNION ALL
SELECT 
    102 as order_id, 2 as customer_id, 150.00 as amount, '2024-01-16' as order_date
UNION ALL
SELECT 
    103 as order_id, 1 as customer_id, 300.00 as amount, '2024-01-17' as order_date;
"""
