"""Regression tests for executor UDF handling.

These tests ensure that specific executor UDF issues are never reintroduced:
1. UDF discovery and registration
2. UDF query processing
3. Source definition handling
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sqlflow.core.executors.local_executor import LocalExecutor


@pytest.fixture
def temp_udf_project_simple():
    """Create a minimal UDF project for unit testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)

        # Create python_udfs directory with a simple UDF
        python_udfs_dir = project_path / "python_udfs"
        python_udfs_dir.mkdir()

        # Create __init__.py
        (python_udfs_dir / "__init__.py").touch()

        # Create simple UDF module
        udf_file = python_udfs_dir / "simple_udfs.py"
        udf_file.write_text(
            """
from sqlflow.udfs import python_scalar_udf

@python_scalar_udf
def test_func(value: str) -> str:
    return f"processed_{value}"
"""
        )

        yield str(project_path)


class TestExecutorUDFRegression:
    """Unit tests for executor UDF regression fixes."""

    def test_udf_discovery_initializes_discovered_udfs(self, temp_udf_project_simple):
        """Test that UDF discovery properly initializes discovered_udfs attribute.

        Regression test for: UDFs not being discovered during initialization
        """
        executor = LocalExecutor(project_dir=temp_udf_project_simple)

        # Must have discovered_udfs attribute
        assert hasattr(executor, "discovered_udfs")
        assert executor.discovered_udfs is not None
        assert isinstance(executor.discovered_udfs, dict)

        # Should have found at least one UDF
        assert len(executor.discovered_udfs) > 0

        # Should have found our test function
        udf_names = list(executor.discovered_udfs.keys())
        assert any("test_func" in name for name in udf_names)

    @patch("sqlflow.core.engines.duckdb.DuckDBEngine.register_python_udf")
    def test_udf_registration_called_during_initialization(
        self, mock_register, temp_udf_project_simple
    ):
        """Test that UDFs are registered with the engine during initialization.

        Regression test for: UDFs discovered but not registered with DuckDB
        """
        executor = LocalExecutor(project_dir=temp_udf_project_simple)

        # register_python_udf should have been called for each discovered UDF
        assert mock_register.call_count > 0

        # Check that our test function was registered
        call_args_list = [call[0] for call in mock_register.call_args_list]
        registered_names = [args[0] for args in call_args_list]

        assert any("test_func" in name for name in registered_names)

    @patch("sqlflow.core.engines.duckdb.DuckDBEngine.process_query_for_udfs")
    def test_udf_query_processing_called_during_sql_execution(self, mock_process_query):
        """Test that UDF query processing is called during SQL execution.

        Regression test for: SQL queries not being processed for UDF calls
        """
        # Mock the process_query_for_udfs to return modified SQL
        mock_process_query.return_value = (
            "CREATE TABLE test AS SELECT processed_name FROM test_table"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            executor = LocalExecutor(project_dir=temp_dir)
            executor.discovered_udfs = {
                "python_udfs.simple_udfs.test_func": lambda x: f"processed_{x}"
            }

            # Mock DuckDB engine and patch process_query_for_udfs on the instance
            executor.duckdb_engine = MagicMock()
            executor.duckdb_engine.execute_query = MagicMock()
            executor.duckdb_engine.process_query_for_udfs = mock_process_query

            # Execute SQL that contains UDF calls
            sql_with_udf = 'CREATE TABLE test AS SELECT PYTHON_FUNC("python_udfs.simple_udfs.test_func", name) as processed_name FROM test_table'

            # Create a mock step dictionary
            mock_step = {"id": "test_step", "type": "transform", "name": "test"}

            executor._execute_sql_query("test", sql_with_udf, mock_step)

            # process_query_for_udfs should have been called
            mock_process_query.assert_called_once()

            # The processed SQL should have been executed
            executor.duckdb_engine.execute_query.assert_called_once()

    def test_source_definition_step_structure_compatibility(self):
        """Test that source definition steps are handled with correct field names.

        Regression test for: "Unknown connector type: source_definition" errors
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            executor = LocalExecutor(project_dir=temp_dir)

            # Test step with correct structure that executor expects
            step = {
                "id": "source_test",
                "type": "source_definition",
                "name": "test_source",
                "source_connector_type": "CSV",  # This is the field executor should read
                "query": {"path": "data/test.csv", "has_header": True},
            }

            # Should not raise an error
            result = executor._execute_source_definition(step)

            assert result["status"] == "success"

            # Should have stored source definition correctly
            assert hasattr(executor, "source_definitions")
            assert "test_source" in executor.source_definitions
            assert executor.source_definitions["test_source"]["connector_type"] == "CSV"

    def test_load_step_routing_to_proper_method(self):
        """Test that load steps with SOURCE definitions route to execute_load_step.

        Regression test for: Load steps using wrong execution method
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            executor = LocalExecutor(project_dir=temp_dir)

            # Mock the execute_load_step method
            with patch.object(executor, "execute_load_step") as mock_execute_load_step:
                mock_execute_load_step.return_value = {"status": "success"}

                # Create load step with source_name and target_table (proper structure)
                step = {
                    "id": "load_test",
                    "type": "load",
                    "source_name": "test_source",
                    "target_table": "test_table",
                    "source_connector_type": "CSV",
                }

                result = executor._execute_step(step)

                # Should have called execute_load_step (not _execute_load)
                mock_execute_load_step.assert_called_once()
                assert result["status"] == "success"

    def test_load_step_fallback_to_legacy_method(self):
        """Test that load steps without proper structure fall back to legacy method.

        This ensures backward compatibility while preferring the new method.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            executor = LocalExecutor(project_dir=temp_dir)

            # Mock the legacy _execute_load method
            with patch.object(executor, "_execute_load") as mock_execute_load:
                mock_execute_load.return_value = {"status": "success"}

                # Create load step without proper structure (legacy format)
                step = {
                    "id": "load_legacy",
                    "type": "load",
                    # Missing source_name and target_table
                }

                result = executor._execute_step(step)

                # Should have called _execute_load (legacy method)
                mock_execute_load.assert_called_once()
                assert result["status"] == "success"


class TestExecutorSourceDefinitionRegression:
    """Tests for source definition handling regressions."""

    def test_source_definition_parameter_location(self):
        """Test that source definition parameters are read from correct location.

        Regression test for: Parameters in wrong field causing lookup failures
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            executor = LocalExecutor(project_dir=temp_dir)

            # Test with parameters in 'query' field (execution plan format)
            step_with_query = {
                "id": "source_test1",
                "type": "source_definition",
                "name": "test_source1",
                "source_connector_type": "CSV",
                "query": {"path": "data/test1.csv", "has_header": True},
            }

            result = executor._execute_source_definition(step_with_query)
            assert result["status"] == "success"
            assert "test_source1" in executor.source_definitions
            assert executor.source_definitions["test_source1"]["params"] == {
                "path": "data/test1.csv",
                "has_header": True,
            }

            # Test with parameters in 'params' field (legacy format)
            step_with_params = {
                "id": "source_test2",
                "type": "source_definition",
                "name": "test_source2",
                "source_connector_type": "CSV",
                "params": {"path": "data/test2.csv", "has_header": True},
            }

            result = executor._execute_source_definition(step_with_params)
            assert result["status"] == "success"
            assert "test_source2" in executor.source_definitions
            assert executor.source_definitions["test_source2"]["params"] == {
                "path": "data/test2.csv",
                "has_header": True,
            }

    def test_source_definition_connector_type_field_compatibility(self):
        """Test backward compatibility for connector_type field names.

        Regression test for: Field name mismatches between planner and executor
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            executor = LocalExecutor(project_dir=temp_dir)

            # Test with source_connector_type (new format)
            step_new = {
                "id": "source_new",
                "type": "source_definition",
                "name": "test_source_new",
                "source_connector_type": "CSV",
                "query": {"path": "data/test.csv"},
            }

            result = executor._execute_source_definition(step_new)
            assert result["status"] == "success"
            assert (
                executor.source_definitions["test_source_new"]["connector_type"]
                == "CSV"
            )

            # Test with just connector_type (legacy format)
            step_legacy = {
                "id": "source_legacy",
                "type": "source_definition",
                "name": "test_source_legacy",
                "connector_type": "CSV",  # No source_ prefix
                "query": {"path": "data/test.csv"},
            }

            result = executor._execute_source_definition(step_legacy)
            assert result["status"] == "success"
            assert (
                executor.source_definitions["test_source_legacy"]["connector_type"]
                == "CSV"
            )
