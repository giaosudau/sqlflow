"""Unit tests for ThreadPoolTaskExecutor UDF integration."""

from unittest import mock

import pandas as pd

from sqlflow.core.executors.thread_pool_executor import ThreadPoolTaskExecutor
from sqlflow.udfs.decorators import python_scalar_udf, python_table_udf


# ----- Test UDF functions for testing -----
@python_scalar_udf
def example_udf_double(x: float) -> float:
    """Double a value."""
    return x * 2


@python_table_udf
def example_udf_add_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add a column to a DataFrame."""
    result = df.copy()
    result["doubled"] = result["value"] * 2
    return result


def test_thread_pool_executor_udf_discovery():
    """Test UDF discovery in ThreadPoolTaskExecutor."""
    # Create a temporary directory with UDFs
    with mock.patch("sqlflow.udfs.manager.PythonUDFManager.discover_udfs") as mock_discover:
        mock_discover.return_value = {"example_udf_double": example_udf_double}
        
        # Initialize executor with project_dir
        executor = ThreadPoolTaskExecutor(project_dir="/test/project")
        
        # Verify UDFs were discovered
        assert mock_discover.called
        assert "example_udf_double" in executor.discovered_udfs


def test_thread_pool_executor_include_step():
    """Test handling of INCLUDE steps for UDFs."""
    executor = ThreadPoolTaskExecutor()
    
    # Mock UDF discovery to avoid file system access
    with mock.patch.object(executor, "discover_udfs") as mock_discover:
        mock_discover.return_value = {"example_udf_double": example_udf_double}
        
        # Test INCLUDE step for Python file
        include_step = {
            "id": "include_1",
            "type": "INCLUDE",
            "file_path": "example_udf.py",
        }
        
        result = executor.execute_step(include_step)
        
        # Verify discovery was triggered and step succeeded
        assert mock_discover.called
        assert result["status"] == "success"


def test_thread_pool_executor_sql_query_udf_extraction():
    """Test extraction of UDFs from SQL queries."""
    executor = ThreadPoolTaskExecutor()
    
    # Set up discovered UDFs
    executor.discovered_udfs = {"example_udf_double": example_udf_double}
    
    # Mock get_udfs_for_query to verify it's called with the right query
    with mock.patch.object(executor, "get_udfs_for_query") as mock_get_udfs:
        mock_get_udfs.return_value = {"example_udf_double": example_udf_double}
        
        # Create a transform step with SQL referencing a UDF
        transform_step = {
            "id": "transform_1",
            "type": "transform",
            "query": "SELECT PYTHON_FUNC(\"example_udf_double\", value) FROM test_table",
        }
        
        result = executor.execute_step(transform_step)
        
        # Verify UDF extraction was called with the SQL query
        mock_get_udfs.assert_called_once_with(transform_step["query"])
        assert result["status"] == "success"


def test_thread_pool_executor_execute_with_udfs():
    """Test execute method with UDF handling."""
    executor = ThreadPoolTaskExecutor()
    
    # Mock UDF discovery
    with mock.patch.object(executor, "discover_udfs") as mock_discover:
        mock_discover.return_value = {"example_udf_double": example_udf_double}
        
        # Mock execute_step to avoid actual execution
        with mock.patch.object(executor, "_execute_with_thread_pool") as mock_execute:
            mock_execute.return_value = {"status": "success"}
            
            # Create a simple plan
            plan = [
                {
                    "id": "transform_1",
                    "type": "transform",
                    "query": "SELECT PYTHON_FUNC(\"example_udf_double\", value) FROM test_table",
                }
            ]
            
            # Execute the plan
            result = executor.execute(plan, project_dir="/test/project")
            
            # Verify UDF discovery was called
            assert mock_discover.called
            # Verify execute_with_thread_pool was called with the plan
            mock_execute.assert_called_once()
            assert result["status"] == "success"
