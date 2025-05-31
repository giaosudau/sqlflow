"""Unit tests for the UDF manager."""

import os
import tempfile
from unittest import mock
from unittest.mock import patch

import pytest

from sqlflow.udfs.decorators import python_scalar_udf
from sqlflow.udfs.manager import PythonUDFManager, UDFDiscoveryError


@pytest.mark.serial
def test_init_default_project_dir():
    """Test that the manager initializes with the current directory by default."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a subdirectory to avoid getcwd issues
        test_dir = os.path.join(temp_dir, "test_project")
        os.makedirs(test_dir)

        # Instead of changing directory, patch os.getcwd to return our test directory
        # This avoids the flaky test issue where parallel tests interfere with cwd
        with patch("os.getcwd", return_value=test_dir):
            manager = PythonUDFManager()
            # Use realpath to handle symlinks (e.g., /var -> /private/var on macOS)
            assert os.path.realpath(manager.project_dir) == os.path.realpath(test_dir)
            assert manager.udfs == {}
            assert manager.udf_info == {}


def test_init_custom_project_dir():
    """Test that the manager initializes with a custom project directory."""
    test_dir = "/test/project/dir"
    manager = PythonUDFManager(project_dir=test_dir)
    assert manager.project_dir == test_dir


def test_discover_udfs_directory_not_found():
    """Test that discover_udfs handles missing directories."""
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = PythonUDFManager(project_dir=temp_dir)
        # No python_udfs directory exists
        with pytest.raises(UDFDiscoveryError):
            manager.discover_udfs(strict=True)

        # Check that error was recorded
        assert "directory_not_found" in manager.discovery_errors

        # Test non-strict mode
        result = manager.discover_udfs(strict=False)
        assert result == {}


def test_discover_udfs_with_sample_module():
    """Test discovering UDFs from a sample module."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a python_udfs directory
        udf_dir = os.path.join(temp_dir, "python_udfs")
        os.makedirs(udf_dir)

        # Create a sample UDF file
        with open(os.path.join(udf_dir, "sample_udf.py"), "w") as f:
            f.write(
                """
from sqlflow.udfs.decorators import python_scalar_udf, python_table_udf
import pandas as pd

@python_scalar_udf
def add_tax(price, tax_rate=0.1):
    return price * (1 + tax_rate)

@python_table_udf(name="custom_name", output_schema={"value": "DOUBLE", "metric": "DOUBLE"})
def calculate_metrics(df):
    result = df.copy()
    result['metric'] = result['value'] * 2
    return result
"""
            )

        # Discover UDFs
        manager = PythonUDFManager(project_dir=temp_dir)
        result = manager.discover_udfs()

        # Check that both expected UDFs were discovered
        assert "python_udfs.sample_udf.add_tax" in result
        assert "python_udfs.sample_udf.custom_name" in result

        # Check UDF info
        assert manager.udf_info["python_udfs.sample_udf.add_tax"]["type"] == "scalar"
        assert manager.udf_info["python_udfs.sample_udf.custom_name"]["type"] == "table"
        assert (
            manager.udf_info["python_udfs.sample_udf.custom_name"]["original_name"]
            == "calculate_metrics"
        )


def test_get_udf():
    """Test getting a UDF by name."""
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = PythonUDFManager(project_dir=temp_dir)

        # Add a test UDF to the manager
        @python_scalar_udf
        def test_func(x):
            return x * 2

        manager.udfs = {"test.func": test_func}

        # Test getting an existing UDF
        assert manager.get_udf("test.func") == test_func

        # Test getting a non-existent UDF
        assert manager.get_udf("nonexistent.func") is None


def test_get_udf_info():
    """Test getting UDF info by name."""
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = PythonUDFManager(project_dir=temp_dir)

        # Add a test UDF info to the manager
        manager.udf_info = {"test.func": {"type": "scalar", "name": "func"}}

        # Test getting existing UDF info
        info = manager.get_udf_info("test.func")
        assert info is not None
        assert info["type"] == "scalar"

        # Test getting non-existent UDF info
        assert manager.get_udf_info("nonexistent.func") is None


def test_list_udfs():
    """Test listing all UDFs."""
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = PythonUDFManager(project_dir=temp_dir)

        # Add test UDFs to the manager
        manager.udfs = {"test.func1": lambda x: x, "test.func2": lambda x: x * 2}

        manager.udf_info = {
            "test.func1": {"type": "scalar", "udf_name": "func1"},
            "test.func2": {"type": "scalar", "udf_name": "func2"},
        }

        # List UDFs
        udf_list = manager.list_udfs()

        # Check result
        assert len(udf_list) == 2
        for item in udf_list:
            assert "name" in item
            assert item["name"] in ["test.func1", "test.func2"]
            assert "type" in item
            assert item["type"] == "scalar"
            assert "udf_name" in item
            assert item["udf_name"] in ["func1", "func2"]


def test_extract_udf_references():
    """Test extracting UDF references from SQL queries."""
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = PythonUDFManager(project_dir=temp_dir)

        # Add test UDFs to the manager
        manager.udfs = {
            "math.add": lambda x, y: x + y,
            "utils.format": lambda x: str(x),
        }

        # Test SQL with UDF references
        sql = """
        SELECT
          PYTHON_FUNC("math.add", a, b) AS sum,
          PYTHON_FUNC("utils.format", c) AS formatted,
          PYTHON_FUNC("unknown.func", d) AS unknown
        FROM table
        """

        # Extract references
        refs = manager.extract_udf_references(sql)

        # Check that only known UDFs were extracted
        assert len(refs) == 2
        assert "math.add" in refs
        assert "utils.format" in refs
        assert "unknown.func" not in refs


def test_get_udfs_for_query():
    """Test getting UDFs for a specific query."""
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = PythonUDFManager(project_dir=temp_dir)

        # Create test UDFs
        @python_scalar_udf
        def add(x, y):
            return x + y

        @python_scalar_udf
        def multiply(x, y):
            return x * y

        # Add UDFs to the manager
        manager.udfs = {"math.add": add, "math.multiply": multiply}

        # Test SQL with UDF references
        sql = """
        SELECT
          PYTHON_FUNC("math.add", a, b) AS sum
        FROM table
        """

        # Get UDFs for query
        query_udfs = manager.get_udfs_for_query(sql)

        # Check that only referenced UDFs were returned
        assert len(query_udfs) == 1
        assert "math.add" in query_udfs
        assert query_udfs["math.add"] == add


class MockEngine:
    def __init__(self):
        self.registered_udfs = {}

    def register_python_udf(self, name, func):
        self.registered_udfs[name] = func


def test_register_udfs_with_engine():
    """Test registering UDFs with an engine."""
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = PythonUDFManager(project_dir=temp_dir)

        # Create test UDFs
        @python_scalar_udf
        def add(x, y):
            return x + y

        @python_scalar_udf
        def multiply(x, y):
            return x * y

        # Add UDFs to the manager
        manager.udfs = {"math.add": add, "math.multiply": multiply}

        # Create a mock engine
        engine = MockEngine()

        # Register UDFs
        manager.register_udfs_with_engine(engine)

        # Check that UDFs were registered
        assert len(engine.registered_udfs) == 2
        assert "math.add" in engine.registered_udfs
        assert "math.multiply" in engine.registered_udfs
        assert engine.registered_udfs["math.add"] == add
        assert engine.registered_udfs["math.multiply"] == multiply

        # Test registering specific UDFs
        engine2 = MockEngine()
        manager.register_udfs_with_engine(engine2, ["math.add"])

        # Check that only the specified UDF was registered
        assert len(engine2.registered_udfs) == 1
        assert "math.add" in engine2.registered_udfs


def test_register_udfs_with_engine_no_support():
    """Test registering UDFs with an engine that doesn't support UDFs."""
    manager = PythonUDFManager()

    # Add a test UDF to the manager
    manager.udfs = {"test.func": lambda x: x}

    # Create a mock engine without register_python_udf method
    engine = object()

    # This should not raise an exception
    with mock.patch("sqlflow.udfs.manager.logger.warning") as mock_warning:
        manager.register_udfs_with_engine(engine)
        mock_warning.assert_called_once()


def test_register_udfs_with_engine_error():
    """Test handling errors when registering UDFs with an engine."""
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = PythonUDFManager(project_dir=temp_dir)

        class ErrorEngine:
            def register_python_udf(self, name, func):
                raise Exception("Registration failed")

        # Add a test UDF
        manager.udfs = {"test.func": lambda x: x}

        # Test error handling - should return registration errors, not raise
        errors = manager.register_udfs_with_engine(ErrorEngine())

        # Should have registration errors
        assert errors is not None
        assert len(errors) == 1
        assert errors[0][0] == "test.func"
        assert "Registration failed" in errors[0][1]
