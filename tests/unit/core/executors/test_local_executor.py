"""Tests for the local executor."""

from unittest.mock import MagicMock

import pytest

# Import executor components for testing
from sqlflow.project import Project


@pytest.fixture
def mock_project():
    """Create a mock project for testing."""
    project = MagicMock(spec=Project)
    profile = {
        "connectors": {
            "postgres": {
                "type": "POSTGRES",
                "params": {
                    "host": "localhost",
                    "port": 5432,
                    "dbname": "test",
                    "user": "test",
                    "password": "test",
                },
            },
            "csv": {
                "type": "CSV",
                "params": {
                    "has_header": True,
                },
            },
        }
    }
    project.get_profile.return_value = profile
    project.profile_name = "dev"
    project.project_dir = "/tmp/testdir"
    return project


@pytest.fixture
def mock_connector_engine():
    """Create a mock connector engine for testing."""
    connector_engine = MagicMock()
    connector_engine.registered_connectors = {}
    return connector_engine


@pytest.fixture
def executor(mock_project, mock_connector_engine):
    """Create a V2 orchestrator for testing with dependencies stubbed."""
    from sqlflow.core.executors.v2.orchestrator import LocalOrchestrator

    # Create actual V2 orchestrator instance
    executor = LocalOrchestrator()

    # The V2 orchestrator stores source definitions differently
    # No need to set non-existent attributes

    return executor


def test_source_definition_step_traditional(executor):
    """Test execution of a source definition step with traditional TYPE PARAMS syntax."""
    step = {
        "id": "source_test",
        "type": "source",
        "name": "test_source",
        "connector_type": "csv",  # V2 uses lowercase
        "params": {  # V2 uses 'params' instead of 'query'
            "path": "data/test.csv",
            "has_header": True,
        },
    }

    result = executor._execute_source_step(step, {})  # V2 method signature
    assert result["status"] == "success"
    assert result["step_id"] == "source_test"

    # Verify that source definition was stored in V2 format
    assert "test_source" in executor.source_definitions
    assert executor.source_definitions["test_source"]["connector_type"] == "csv"
    assert (
        executor.source_definitions["test_source"]["params"]["path"] == "data/test.csv"
    )


def test_source_definition_step_profile_based(executor):
    """Test execution of a source definition step with profile-based FROM syntax."""
    step = {
        "id": "source_test",
        "type": "source",
        "name": "test_pg_source",
        "connector_type": "postgres",  # V2 explicit connector type
        "params": {  # V2 uses 'params' instead of 'query'
            "table": "users",
        },
    }

    result = executor._execute_source_step(step, {})  # V2 method signature
    assert result["status"] == "success"
    assert result["step_id"] == "source_test"

    # Verify that source definition was stored in V2 format
    assert "test_pg_source" in executor.source_definitions
    assert executor.source_definitions["test_pg_source"]["connector_type"] == "postgres"
    assert executor.source_definitions["test_pg_source"]["params"]["table"] == "users"
