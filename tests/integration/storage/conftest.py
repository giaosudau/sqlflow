"""Storage-specific fixtures for integration tests."""

import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.core.storage.artifact_manager import ArtifactManager
from sqlflow.core.storage.duckdb_state_backend import DuckDBStateBackend


@pytest.fixture(scope="function")
def temp_storage_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for storage tests.

    Yields
    ------
        Path to temporary storage directory

    """
    with tempfile.TemporaryDirectory(prefix="sqlflow_storage_test_") as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture(scope="function")
def artifact_manager(temp_storage_dir: Path) -> Generator[ArtifactManager, None, None]:
    """Create an ArtifactManager for testing.

    Args:
    ----
        temp_storage_dir: Temporary directory fixture

    Yields:
    ------
        Configured ArtifactManager instance

    """
    manager = ArtifactManager(project_dir=str(temp_storage_dir))
    yield manager


@pytest.fixture(scope="function")
def duckdb_engine_with_storage() -> Generator[DuckDBEngine, None, None]:
    """Create a DuckDB engine for storage testing.

    Yields:
    ------
        DuckDBEngine configured for storage tests

    """
    engine = DuckDBEngine(database_path=":memory:")
    yield engine
    engine.close()


@pytest.fixture(scope="function")
def state_backend(temp_storage_dir: Path) -> Generator[DuckDBStateBackend, None, None]:
    """Create a DuckDBStateBackend for testing.

    Args:
    ----
        temp_storage_dir: Temporary directory fixture

    Yields:
    ------
        Configured DuckDBStateBackend instance

    """
    db_path = str(temp_storage_dir / "state.db")
    backend = DuckDBStateBackend(db_path=db_path)
    yield backend


@pytest.fixture(scope="function")
def persistent_storage_setup() -> Generator[dict[str, Any], None, None]:
    """Create a complete persistent storage setup for testing.

    Yields:
    ------
        Dictionary with storage components and configuration

    """
    with tempfile.TemporaryDirectory(prefix="sqlflow_persistent_storage_") as tmp_dir:
        # Create directory structure
        storage_dir = Path(tmp_dir)
        artifacts_dir = storage_dir / "artifacts"
        db_path = storage_dir / "state.duckdb"

        artifacts_dir.mkdir(parents=True, exist_ok=True)

        # Create components
        artifact_manager = ArtifactManager(project_dir=str(artifacts_dir))
        engine = DuckDBEngine(database_path=str(db_path))
        state_backend = DuckDBStateBackend(db_path=str(db_path))

        yield {
            "storage_dir": storage_dir,
            "artifacts_dir": artifacts_dir,
            "db_path": db_path,
            "artifact_manager": artifact_manager,
            "engine": engine,
            "state_backend": state_backend,
        }

        # Cleanup
        engine.close()


@pytest.fixture(scope="function")
def sample_artifacts() -> dict[str, Any]:
    """Create sample artifacts for testing.

    Returns:
    -------
        Dictionary with sample artifact data

    """
    return {
        "json_data": {"key": "value", "number": 42, "list": [1, 2, 3]},
        "text_data": "This is a sample text file content\nwith multiple lines\nfor testing.",
        "binary_data": b"Binary content for testing\x00\x01\x02",
        "metadata": {
            "created_by": "test_user",
            "purpose": "integration_testing",
            "version": "1.0.0",
        },
    }
