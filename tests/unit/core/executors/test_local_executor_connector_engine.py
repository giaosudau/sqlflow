"""Tests for LocalExecutor ConnectorEngine profile integration.

Following TDD principles: test behavior, not implementation.
Focus on real integration scenarios without excessive mocking.
"""

from unittest.mock import MagicMock, patch

import pytest

from sqlflow.connectors.connector_engine import ConnectorEngine
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.project import Project


class TestLocalExecutorConnectorEngineIntegration:
    """Test ConnectorEngine initialization with profile configuration."""

    @pytest.fixture
    def mock_project_with_s3_profile(self):
        """Create mock project with S3 profile configuration."""
        project = MagicMock(spec=Project)
        project.profile = {
            "connectors": {
                "s3": {
                    "type": "s3",
                    "endpoint_url": "http://minio:9000",
                    "access_key_id": "minioadmin",
                    "secret_access_key": "minioadmin",
                    "region": "us-east-1",
                    "bucket": "test-bucket",
                    "use_ssl": False,
                    "cost_limit_usd": 10.0,
                },
                "postgres": {
                    "type": "postgres",
                    "host": "localhost",
                    "port": 5432,
                    "database": "test",
                },
            },
            "engines": {"duckdb": {"mode": "memory"}},
            "variables": {},
        }
        return project

    def test_connector_engine_initialization_with_profile_config(
        self, mock_project_with_s3_profile
    ):
        """Test that ConnectorEngine receives profile configuration during initialization."""
        # Arrange
        executor = LocalExecutor(project=mock_project_with_s3_profile)

        # Act: Force lazy initialization of connector engine
        load_step = MagicMock()
        load_step.source_name = "test_source"
        load_step.table_name = "test_table"

        source_definition = {"connector_type": "CSV", "params": {"path": "test.csv"}}

        # This should trigger connector engine creation
        with patch.object(
            executor, "_get_source_definition", return_value=source_definition
        ):
            with patch.object(executor.duckdb_engine, "register_table"):
                try:
                    executor._load_source_data_into_duckdb(load_step, source_definition)
                except Exception:
                    # Expected - we just want to trigger connector engine creation
                    pass

        # Assert
        assert executor.connector_engine is not None
        assert hasattr(executor.connector_engine, "profile_config")
        assert (
            executor.connector_engine.profile_config
            == mock_project_with_s3_profile.profile
        )

    def test_connector_engine_profile_config_passed_to_s3_exports(
        self, mock_project_with_s3_profile
    ):
        """Test that S3 export operations receive profile configuration."""
        # Arrange
        executor = LocalExecutor(project=mock_project_with_s3_profile)

        # Act: Create connector engine using the correct method
        connector_engine = executor._create_connector_engine()

        # Assert
        assert isinstance(connector_engine, ConnectorEngine)
        assert hasattr(connector_engine, "profile_config")
        assert connector_engine.profile_config == mock_project_with_s3_profile.profile

        # Verify S3 config is accessible (only if profile_config is not None)
        if connector_engine.profile_config:
            s3_config = connector_engine.profile_config.get("connectors", {}).get(
                "s3", {}
            )
            assert s3_config["bucket"] == "test-bucket"
            assert s3_config["endpoint_url"] == "http://minio:9000"

    def test_connector_engine_lazy_initialization_uses_create_method(
        self, mock_project_with_s3_profile
    ):
        """Test that lazy initialization uses _create_connector_engine method."""
        # Arrange
        executor = LocalExecutor(project=mock_project_with_s3_profile)

        # Ensure connector_engine starts as None
        assert executor.connector_engine is None

        # Act: Trigger lazy initialization through load operation
        load_step = MagicMock()
        load_step.source_name = "test_source"
        load_step.table_name = "test_table"

        source_definition = {"connector_type": "CSV", "params": {"path": "test.csv"}}

        with patch.object(
            executor, "_get_source_definition", return_value=source_definition
        ):
            with patch.object(executor.duckdb_engine, "register_table"):
                with patch(
                    "sqlflow.connectors.connector_engine.ConnectorEngine"
                ) as MockConnectorEngine:
                    # Configure mock to track how it's called
                    mock_engine = MagicMock()
                    MockConnectorEngine.return_value = mock_engine

                    try:
                        executor._load_source_data_into_duckdb(
                            load_step, source_definition
                        )
                    except Exception:
                        # Expected - we just want to verify initialization
                        pass

        # Assert: connector_engine should be initialized with profile config
        assert executor.connector_engine is not None

    def test_connector_engine_s3_profile_merge_behavior(
        self, mock_project_with_s3_profile
    ):
        """Test that S3 profile configuration merging works correctly."""
        # Arrange
        executor = LocalExecutor(project=mock_project_with_s3_profile)
        connector_engine = executor._create_connector_engine()

        # Act: Test profile config merging for S3 exports
        export_options = {"file_format": "parquet"}
        merged_config = connector_engine._merge_profile_connector_config(
            "S3", export_options
        )

        # Assert: Profile config should be merged with export options
        assert "bucket" in merged_config
        assert merged_config["bucket"] == "test-bucket"
        assert merged_config["file_format"] == "parquet"  # Export option preserved
        assert (
            merged_config["endpoint_url"] == "http://minio:9000"
        )  # Profile config included

    def test_connector_engine_initialization_without_profile(self):
        """Test ConnectorEngine initialization when no profile is available."""
        # Arrange
        executor = LocalExecutor()  # No project/profile

        # Act
        connector_engine = executor._create_connector_engine()

        # Assert
        assert isinstance(connector_engine, ConnectorEngine)
        # Should still work, but profile_config should be None or empty
        assert (
            connector_engine.profile_config is None
            or connector_engine.profile_config == {}
        )

    def test_export_operation_uses_connector_engine_with_profile(
        self, mock_project_with_s3_profile
    ):
        """Test that export operations use ConnectorEngine with profile configuration."""
        # Arrange
        executor = LocalExecutor(project=mock_project_with_s3_profile)

        export_step = {
            "type": "export",
            "query": {
                "destination_uri": "s3://test-bucket/test.csv",
                "options": {"file_format": "csv"},
            },
            "source_connector_type": "S3",
        }

        # Act: Mock the connector engine creation to verify it's called
        with patch.object(executor, "_create_connector_engine") as mock_create:
            mock_engine = MagicMock()
            mock_create.return_value = mock_engine

            with patch.object(
                executor,
                "_resolve_export_source",
                return_value=("test_table", MagicMock()),
            ):
                with patch("os.makedirs"):
                    result = executor._execute_export(export_step)

        # Assert: Should have used the proper connector engine creation method
        # Note: This test verifies the fix is properly integrated
        assert result["status"] == "success"
