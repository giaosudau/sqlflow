"""Tests for the local executor."""

from unittest.mock import MagicMock

import pytest

# Import LocalExecutor at module level
from sqlflow.core.executors.local_executor import LocalExecutor
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
    """Create a mock executor for testing with dependencies stubbed."""
    # Create a partial LocalExecutor instance with mocks
    executor = MagicMock(spec=LocalExecutor)

    # Set required attributes
    executor.project = mock_project
    executor.profile_name = "dev"
    executor.connector_engine = mock_connector_engine
    executor.source_connectors = {}
    executor.step_table_map = {}

    # Set up return values for the refactored methods
    executor._handle_traditional_source.return_value = {"status": "success"}
    executor._handle_profile_based_source.return_value = {"status": "success"}

    # Import the real implementation method to test
    # Get the actual method from the class and bind it to our mock
    executor._execute_source_definition = (
        LocalExecutor._execute_source_definition.__get__(executor)
    )

    return executor


def test_source_definition_step_traditional(executor):
    """Test execution of a source definition step with traditional TYPE PARAMS syntax."""
    step = {
        "id": "source_test",
        "type": "source_definition",
        "name": "test_source",
        "source_connector_type": "CSV",
        "query": {
            "path": "data/test.csv",
            "has_header": True,
        },
        "depends_on": [],
    }

    result = executor._execute_source_definition(step)
    assert result["status"] == "success"

    # Verify the correct helper method was called with the right parameters
    executor._handle_traditional_source.assert_called_once_with(
        step, step["id"], step["name"]
    )


def test_source_definition_step_profile_based(executor):
    """Test execution of a source definition step with profile-based FROM syntax."""
    step = {
        "id": "source_test",
        "type": "source_definition",
        "name": "test_pg_source",
        "is_from_profile": True,
        "profile_connector_name": "postgres",
        "query": {
            "table": "users",
        },
        "depends_on": [],
    }

    result = executor._execute_source_definition(step)
    assert result["status"] == "success"

    # Verify the correct helper method was called with the right parameters
    executor._handle_profile_based_source.assert_called_once_with(
        step, step["id"], step["name"]
    )
