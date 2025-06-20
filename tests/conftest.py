"""Pytest configuration for SQLFlow tests."""

import tempfile
from typing import Any, Dict, Generator

import pandas as pd
import pytest

from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.variables import VariableConfig


@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    """Create a temporary directory for tests.

    Yields
    ------
        Path to the temporary directory

    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def sample_source_directive() -> str:
    """Return a sample SOURCE directive for testing.

    Returns
    -------
        Sample SOURCE directive text

    """
    return """SOURCE users TYPE POSTGRES PARAMS {
        "connection": "postgresql://user:pass@localhost:5432/db",
        "table": "users"
    };"""


@pytest.fixture
def sample_pipeline() -> str:
    """Return a sample pipeline for testing.

    Returns
    -------
        Sample pipeline text

    """
    return """SOURCE users TYPE POSTGRES PARAMS {
        "connection": "postgresql://user:pass@localhost:5432/db",
        "table": "users"
    };
    SOURCE sales TYPE CSV PARAMS {
        "path": "data/sales.csv",
        "has_header": true
    };"""


@pytest.fixture
def sample_csv_data() -> str:
    """Return sample CSV data for testing.

    Returns
    -------
        Sample CSV data as string

    """
    return "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n"


@pytest.fixture
def sample_csv_data_with_headers() -> str:
    """Return sample CSV data with headers for testing.

    Returns
    -------
        Sample CSV data with headers as string

    """
    return "user_id,full_name,email,age\n1,John Doe,john@email.com,25\n2,Jane Smith,jane@email.com,30\n"


@pytest.fixture
def test_variable_config() -> VariableConfig:
    """Return a test variable configuration.

    Returns
    -------
        VariableConfig instance for testing

    """
    return VariableConfig(
        cli_variables={"env": "test", "table": "users", "debug": "true"},
        profile_variables={"db_host": "localhost", "db_port": "5432"},
    )


@pytest.fixture
def test_variables_dict() -> Dict[str, Any]:
    """Return a simple variables dictionary for testing.

    Returns
    -------
        Dictionary of test variables

    """
    return {
        "env": "test",
        "table": "test_table",
        "debug": True,
        "count": 42,
        "database_url": "postgresql://user:pass@localhost:5432/testdb",
    }


@pytest.fixture
def in_memory_engine() -> DuckDBEngine:
    """Return an in-memory DuckDB engine for testing.

    Returns
    -------
        DuckDBEngine instance configured for in-memory usage

    """
    return DuckDBEngine(":memory:")


@pytest.fixture
def sample_json_variables() -> str:
    """Return sample JSON variables string for testing.

    Returns
    -------
        JSON string containing test variables

    """
    return '{"env": "prod", "debug": true, "count": 42, "table": "users"}'


@pytest.fixture
def sample_key_value_variables() -> str:
    """Return sample key=value variables string for testing.

    Returns
    -------
        Key=value string containing test variables

    """
    return "env=prod,debug=true,count=42,table=users"


@pytest.fixture
def sample_sql_template() -> str:
    """Return a sample SQL template with variables for testing.

    Returns
    -------
        SQL template string with variable placeholders

    """
    return "SELECT * FROM {{table}} WHERE env = '{{env}}' AND debug = {{debug}}"


@pytest.fixture
def sample_pipeline_operations() -> list:
    """Return sample pipeline operations for testing.

    Returns
    -------
        List of pipeline operation dictionaries

    """
    return [
        {
            "type": "source",
            "name": "users",
            "connector": "postgres",
            "params": {
                "connection": "postgresql://user:pass@localhost:5432/db",
                "table": "users",
            },
        },
        {
            "type": "source",
            "name": "sales",
            "connector": "csv",
            "params": {"path": "data/sales.csv", "has_header": True},
        },
        {
            "type": "transform",
            "name": "user_sales",
            "sql": "SELECT u.name, s.amount FROM users u JOIN sales s ON u.id = s.user_id",
        },
    ]


@pytest.fixture
def sample_csv_config() -> Dict[str, Any]:
    """Return sample CSV connector configuration for testing.

    Returns
    -------
        Dictionary containing CSV connector configuration

    """
    return {
        "type": "csv",
        "path": "test.csv",
        "delimiter": ",",
        "has_header": True,
        "encoding": "utf-8",
    }


@pytest.fixture
def sample_postgres_config() -> Dict[str, Any]:
    """Return sample Postgres connector configuration for testing.

    Returns
    -------
        Dictionary containing Postgres connector configuration

    """
    return {
        "type": "postgres",
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "username": "testuser",
        "password": "testpass",
        "table": "test_table",
    }


# Phase 3: Executor Fixtures for V2 Default Testing Infrastructure
# Following Kent Beck's TDD principles and clean testing standards


@pytest.fixture
def local_executor():
    """Standard executor fixture for all tests."""
    from sqlflow.core.executors import get_executor

    return get_executor()


@pytest.fixture
def performance_test_data(tmp_path):
    """Generates consistent test datasets for performance validation."""

    # Create small dataset (100 rows)
    small_data = pd.DataFrame(
        {
            "id": range(1, 101),
            "name": [f"user_{i}" for i in range(1, 101)],
            "value": [i * 2.5 for i in range(1, 101)],
            "category": [f"cat_{i%5}" for i in range(1, 101)],
        }
    )

    # Create medium dataset (10K rows)
    medium_data = pd.DataFrame(
        {
            "id": range(1, 10001),
            "name": [f"user_{i}" for i in range(1, 10001)],
            "value": [i * 2.5 for i in range(1, 10001)],
            "category": [f"cat_{i%10}" for i in range(1, 10001)],
        }
    )

    # Save to temporary files
    small_file = tmp_path / "small_dataset.csv"
    medium_file = tmp_path / "medium_dataset.csv"

    small_data.to_csv(small_file, index=False)
    medium_data.to_csv(medium_file, index=False)

    return {
        "small_file": small_file,
        "medium_file": medium_file,
        "small_rows": len(small_data),
        "medium_rows": len(medium_data),
    }
