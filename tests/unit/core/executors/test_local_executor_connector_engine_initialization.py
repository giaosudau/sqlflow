"""Test local executor connector engine initialization during export operations.

This module tests the connector engine initialization behavior in LocalExecutor,
specifically ensuring that the connector engine is properly initialized for
export operations and not just load operations.
"""

from unittest.mock import Mock, patch

from sqlflow.core.errors import ConnectorError
from sqlflow.core.executors.local_executor import LocalExecutor


class TestLocalExecutorConnectorEngineInitialization:
    """Test class for local executor connector engine initialization behavior."""

    def test_connector_engine_initialized_during_export_operation(self):
        """Test that connector engine is properly initialized during export operations.

        This test verifies that the connector engine is created when executing
        export operations if it doesn't already exist, ensuring exports can
        access the connector registry properly.
        """
        executor = LocalExecutor()

        # Mock the connector engine creation
        mock_connector_engine = Mock()
        mock_export_connector = Mock()
        mock_connector_engine._get_or_create_export_connector.return_value = (
            mock_export_connector
        )

        with patch.object(
            executor, "_create_connector_engine", return_value=mock_connector_engine
        ):
            # Create a test export step
            export_step = {
                "type": "export",
                "query": {"sql_query": "SELECT * FROM test_table"},
                "destination": "test_output.csv",
                "connector_type": "CSV",
                "options": {"header": True},
            }

            # Mock the query execution to return test data with proper result structure
            test_data = Mock()
            test_data.fetchall.return_value = [("test_id", "test_value")]
            test_data.description = [("id",), ("value",)]

            with patch.object(
                executor.duckdb_engine, "execute_query", return_value=test_data
            ):
                # Execute the export step
                result = executor._execute_export(export_step)

                # Verify that connector engine was created
                assert executor.connector_engine is not None
                assert executor.connector_engine == mock_connector_engine

                # Verify the export operation completed
                assert result["status"] == "success"

    def test_connector_engine_reused_if_already_initialized(self):
        """Test that existing connector engine is reused during export operations.

        This ensures we don't recreate the connector engine unnecessarily if it
        already exists from previous load operations or other exports.
        """
        executor = LocalExecutor()

        # Pre-initialize the connector engine
        mock_existing_connector_engine = Mock()
        executor.connector_engine = mock_existing_connector_engine

        export_step = {
            "type": "export",
            "query": {"sql_query": "SELECT * FROM test_table"},
            "destination": "test_output.csv",
            "connector_type": "CSV",
            "options": {"header": True},
        }

        test_data = Mock()
        test_data.fetchall.return_value = [("test_id", "test_value")]
        test_data.description = [("id",), ("value",)]

        with patch.object(
            executor.duckdb_engine, "execute_query", return_value=test_data
        ):
            with patch("sqlflow.connectors.data_chunk.DataChunk") as mock_data_chunk:
                mock_chunk_instance = Mock()
                mock_data_chunk.return_value = mock_chunk_instance

                with patch.object(executor, "_create_connector_engine") as mock_create:
                    # Execute the export step
                    result = executor._execute_export(export_step)

                    # Verify it succeeded
                    assert result["status"] == "success"

                    # Verify connector engine was NOT recreated
                    mock_create.assert_not_called()
                    assert executor.connector_engine is mock_existing_connector_engine

                    # Verify export was called
                    mock_existing_connector_engine.export_data.assert_called_once()

    def test_export_fails_gracefully_when_connector_engine_creation_fails(self):
        """Test that export fails gracefully when connector engine creation fails.

        This ensures proper error handling when the connector engine cannot be
        initialized during export operations.
        """
        executor = LocalExecutor()

        export_step = {
            "type": "export",
            "query": "SELECT * FROM test_table",
            "destination": "test_output.csv",
            "connector_type": "CSV",
            "options": {"header": True},
        }

        # Mock connector engine creation to fail
        with patch.object(
            executor,
            "_create_connector_engine",
            side_effect=ConnectorError("CSV", "Failed to create"),
        ):
            test_data = Mock()
            with patch.object(
                executor.duckdb_engine, "execute_query", return_value=test_data
            ):
                # Execute should return error status instead of raising
                result = executor._execute_export(export_step)

                assert result["status"] == "error"
                assert "Failed to create" in result["message"]

    def test_connector_engine_initialization_occurs_before_query_execution_failure(
        self,
    ):
        """Test connector engine initialization happens even when query execution fails.

        This tests that the connector engine initialization step occurs early
        in the export process, before query execution, ensuring consistent
        behavior regardless of query success or failure.
        """
        executor = LocalExecutor()

        export_step = {
            "type": "export",
            "query": {"sql_query": "SELECT * FROM nonexistent_table"},
            "destination": "test_output.csv",
            "connector_type": "CSV",
            "options": {"header": True},
        }

        # Mock query execution to fail
        with patch.object(
            executor.duckdb_engine,
            "execute_query",
            side_effect=Exception("Table not found"),
        ):
            with patch.object(executor, "_create_connector_engine") as mock_create:
                mock_connector_engine = Mock()
                mock_create.return_value = mock_connector_engine

                # Execute should now fail properly due to enhanced error propagation
                result = executor._execute_export(export_step)

                assert result["status"] == "error"
                assert "Table not found" in result["message"]
                # Connector engine should still be initialized before the query failure
                mock_create.assert_called_once()

    def test_export_with_empty_query_result_still_initializes_connector_engine(self):
        """Test connector engine initialization with empty query results.

        This ensures that empty query results are handled correctly and the
        connector engine is still properly initialized and used.
        """
        executor = LocalExecutor()

        export_step = {
            "type": "export",
            "query": {"sql_query": "SELECT * FROM empty_table"},
            "destination": "empty_output.csv",
            "connector_type": "CSV",
            "options": {"header": True},
        }

        # Mock empty query result
        empty_data = Mock()
        empty_data.fetchall.return_value = []
        empty_data.description = [("id",), ("value",)]

        mock_connector_engine = Mock()
        mock_export_connector = Mock()
        mock_connector_engine._get_or_create_export_connector.return_value = (
            mock_export_connector
        )

        with patch.object(
            executor, "_create_connector_engine", return_value=mock_connector_engine
        ) as mock_create_engine:
            with patch.object(
                executor.duckdb_engine, "execute_query", return_value=empty_data
            ):
                with patch(
                    "sqlflow.connectors.data_chunk.DataChunk"
                ) as mock_data_chunk:
                    mock_chunk_instance = Mock()
                    mock_data_chunk.return_value = mock_chunk_instance

                    # Execute the export step
                    result = executor._execute_export(export_step)

                    # Should still succeed with empty data
                    assert result["status"] == "success"

                    # Verify connector engine was initialized
                    mock_create_engine.assert_called_once()

                    # Verify export was called even with empty data
                    mock_connector_engine.export_data.assert_called_once()

                    # Check the call arguments match expected pattern
                    call_args = mock_connector_engine.export_data.call_args
                    assert call_args.kwargs["destination"] == "empty_output.csv"
                    assert call_args.kwargs["connector_type"] == "CSV"
