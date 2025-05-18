"""Unit tests for enhanced UDF discovery and metadata features."""

import os
import tempfile

import pytest

from sqlflow.udfs.manager import PythonUDFManager, UDFDiscoveryError


def test_recursive_discovery():
    """Test that UDFs are discovered in subdirectories."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create python_udfs directory with subdirectories
        udf_dir = os.path.join(temp_dir, "python_udfs")
        sub_dir = os.path.join(udf_dir, "subdir")
        nested_dir = os.path.join(sub_dir, "nested")

        os.makedirs(udf_dir)
        os.makedirs(sub_dir)
        os.makedirs(nested_dir)

        # Create UDF files in each directory
        root_file = os.path.join(udf_dir, "root_udf.py")
        subdir_file = os.path.join(sub_dir, "sub_udf.py")
        nested_file = os.path.join(nested_dir, "nested_udf.py")

        # Create UDF in root directory
        with open(root_file, "w") as f:
            f.write(
                """
from sqlflow.udfs.decorators import python_scalar_udf

@python_scalar_udf
def root_func(x: int) -> int:
    \"\"\"Root UDF function.\"\"\"
    return x * 2
"""
            )

        # Create UDF in subdirectory
        with open(subdir_file, "w") as f:
            f.write(
                """
from sqlflow.udfs.decorators import python_scalar_udf

@python_scalar_udf
def sub_func(x: int) -> int:
    \"\"\"Subdirectory UDF function.\"\"\"
    return x * 3
"""
            )

        # Create UDF in nested subdirectory
        with open(nested_file, "w") as f:
            f.write(
                """
from sqlflow.udfs.decorators import python_scalar_udf

@python_scalar_udf
def nested_func(x: int) -> int:
    \"\"\"Nested UDF function.\"\"\"
    return x * 4
"""
            )

        # Initialize UDF manager and discover UDFs
        manager = PythonUDFManager(project_dir=temp_dir)
        udfs = manager.discover_udfs()

        # Verify that UDFs were discovered in all directories
        assert "python_udfs.root_udf.root_func" in udfs
        assert "python_udfs.subdir.sub_udf.sub_func" in udfs
        assert "python_udfs.subdir.nested.nested_udf.nested_func" in udfs

        # Verify proper module naming
        root_info = manager.get_udf_info("python_udfs.root_udf.root_func")
        sub_info = manager.get_udf_info("python_udfs.subdir.sub_udf.sub_func")
        nested_info = manager.get_udf_info(
            "python_udfs.subdir.nested.nested_udf.nested_func"
        )

        assert root_info["module"] == "python_udfs.root_udf"
        assert sub_info["module"] == "python_udfs.subdir.sub_udf"
        assert nested_info["module"] == "python_udfs.subdir.nested.nested_udf"


def test_enhanced_metadata_extraction():
    """Test that comprehensive metadata is extracted from UDFs."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create python_udfs directory
        udf_dir = os.path.join(temp_dir, "python_udfs")
        os.makedirs(udf_dir)

        # Create UDF file with comprehensive metadata
        udf_file = os.path.join(udf_dir, "enhanced_udfs.py")

        with open(udf_file, "w") as f:
            f.write(
                """
from sqlflow.udfs.decorators import python_scalar_udf, python_table_udf
import pandas as pd
from typing import List, Dict, Optional

@python_scalar_udf
def advanced_scalar_udf(x: int, y: float = 1.0, z: str = "default") -> float:
    \"\"\"Advanced scalar UDF with multiple parameters and defaults.
    
    This function demonstrates rich metadata extraction capabilities.
    
    Args:
        x: First parameter
        y: Second parameter with default
        z: String parameter with default
        
    Returns:
        Computed result
    \"\"\"
    return x * y

@python_table_udf(required_columns=["value"])
def advanced_table_udf(df: pd.DataFrame, multiplier: float = 2.0) -> pd.DataFrame:
    \"\"\"Advanced table UDF with DataFrame input/output.
    
    Demonstrates table UDF metadata extraction.
    
    Args:
        df: Input DataFrame
        multiplier: Value to multiply by
        
    Returns:
        Transformed DataFrame
    \"\"\"
    result = df.copy()
    result["doubled"] = result["value"] * multiplier
    return result
"""
            )

        # Initialize UDF manager and discover UDFs
        manager = PythonUDFManager(project_dir=temp_dir)
        manager.discover_udfs()

        # Verify scalar UDF metadata
        scalar_info = manager.get_udf_info(
            "python_udfs.enhanced_udfs.advanced_scalar_udf"
        )
        assert scalar_info is not None
        assert scalar_info["type"] == "scalar"
        assert (
            scalar_info["docstring_summary"]
            == "Advanced scalar UDF with multiple parameters and defaults."
        )

        # Check parameter details
        param_details = scalar_info["param_details"]
        assert "x" in param_details
        assert param_details["x"]["type"] == "int"
        assert param_details["x"]["has_default"] is False

        assert "y" in param_details
        assert param_details["y"]["type"] == "float"
        assert param_details["y"]["has_default"] is True
        assert param_details["y"]["default"] == 1.0

        assert "z" in param_details
        assert param_details["z"]["type"] == "str"
        assert param_details["z"]["has_default"] is True
        assert param_details["z"]["default"] == "default"

        # Verify table UDF metadata
        table_info = manager.get_udf_info(
            "python_udfs.enhanced_udfs.advanced_table_udf"
        )
        assert table_info is not None
        assert table_info["type"] == "table"
        assert (
            table_info["docstring_summary"]
            == "Advanced table UDF with DataFrame input/output."
        )
        assert table_info["required_columns"] == ["value"]

        # Check parameter details for DataFrame parameter
        table_param_details = table_info["param_details"]
        assert "df" in table_param_details
        # Check for DataFrame in annotation string instead of type
        assert "DataFrame" in table_param_details["df"]["annotation"]

        # Check formatted signature
        assert "DataFrame" in table_info["formatted_signature"]
        assert "multiplier:float=2.0" in table_info["formatted_signature"].replace(
            " ", ""
        )

        # Test validation
        validation_warnings = manager.validate_udf_metadata(
            "python_udfs.enhanced_udfs.advanced_table_udf"
        )
        assert len(validation_warnings) == 0


def test_error_handling():
    """Test error handling during UDF discovery."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create python_udfs directory
        udf_dir = os.path.join(temp_dir, "python_udfs")
        os.makedirs(udf_dir)

        # Create UDF file with syntax error
        error_file = os.path.join(udf_dir, "error_udf.py")
        with open(error_file, "w") as f:
            f.write(
                """
from sqlflow.udfs.decorators import python_scalar_udf

@python_scalar_udf
def broken_func(x):
    # Syntax error
    return x + +
"""
            )

        # Create UDF file with runtime error
        runtime_error_file = os.path.join(udf_dir, "runtime_error_udf.py")
        with open(runtime_error_file, "w") as f:
            f.write(
                """
from sqlflow.udfs.decorators import python_scalar_udf
import nonexistent_module  # Import error

@python_scalar_udf
def runtime_error_func(x: int) -> int:
    return x * 2
"""
            )

        # Create valid UDF file to ensure discovery continues after errors
        valid_file = os.path.join(udf_dir, "valid_udf.py")
        with open(valid_file, "w") as f:
            f.write(
                """
from sqlflow.udfs.decorators import python_scalar_udf

@python_scalar_udf
def valid_func(x: int) -> int:
    \"\"\"Valid function.\"\"\"
    return x * 2
"""
            )

        # Initialize UDF manager and discover UDFs
        manager = PythonUDFManager(project_dir=temp_dir)
        udfs = manager.discover_udfs()

        # Check that errors were captured
        discovery_errors = manager.get_discovery_errors()
        assert len(discovery_errors) >= 2  # At least two errors

        # Verify that valid UDF was still discovered
        assert "python_udfs.valid_udf.valid_func" in udfs

        # Check error messages
        error_keys = discovery_errors.keys()
        assert any(error_file in key for key in error_keys)
        assert any(runtime_error_file in key for key in error_keys)


def test_nonexistent_directory():
    """Test handling of nonexistent UDF directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Initialize UDF manager with non-existent directory
        manager = PythonUDFManager(project_dir=temp_dir)

        # Test with strict=True should raise exception
        with pytest.raises(UDFDiscoveryError):
            manager.discover_udfs(python_udfs_dir="nonexistent_dir", strict=True)

        # Check that error was captured
        discovery_errors = manager.get_discovery_errors()
        assert "directory_not_found" in discovery_errors

        # Test with strict=False should return empty dict and not raise
        result = manager.discover_udfs(python_udfs_dir="nonexistent_dir", strict=False)
        assert result == {}


def test_udf_reference_extraction():
    """Test extraction of UDF references from SQL query."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create python_udfs directory
        udf_dir = os.path.join(temp_dir, "python_udfs")
        os.makedirs(udf_dir)

        # Create UDF file
        udf_file = os.path.join(udf_dir, "ref_udfs.py")
        with open(udf_file, "w") as f:
            f.write(
                """
from sqlflow.udfs.decorators import python_scalar_udf

@python_scalar_udf
def ref_func1(x: int) -> int:
    return x * 2

@python_scalar_udf
def ref_func2(x: int) -> int:
    return x * 3
"""
            )

        # Initialize UDF manager and discover UDFs
        manager = PythonUDFManager(project_dir=temp_dir)
        manager.discover_udfs()

        # Test SQL with UDF references
        sql = """
SELECT 
    PYTHON_FUNC("python_udfs.ref_udfs.ref_func1", column1) as col1,
    PYTHON_FUNC('python_udfs.ref_udfs.ref_func2', column2) as col2
FROM my_table
"""
        refs = manager.extract_udf_references(sql)
        assert len(refs) == 2
        assert "python_udfs.ref_udfs.ref_func1" in refs
        assert "python_udfs.ref_udfs.ref_func2" in refs

        # Test getting UDFs for query
        query_udfs = manager.get_udfs_for_query(sql)
        assert len(query_udfs) == 2
        assert "python_udfs.ref_udfs.ref_func1" in query_udfs
        assert "python_udfs.ref_udfs.ref_func2" in query_udfs
