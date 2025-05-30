"""UDF-specific fixtures for integration tests."""

from pathlib import Path
from typing import Any, Dict, Generator

import pytest

from sqlflow.udfs.manager import PythonUDFManager


@pytest.fixture(scope="function")
def udf_test_environment(
    temp_udf_directory: Dict[str, Any],
) -> Generator[Dict[str, Any], None, None]:
    """Create a comprehensive UDF test environment.

    Args:
    ----
        temp_udf_directory: Temporary directory fixture from parent conftest

    Yields:
    ------
        Dictionary with UDF test environment paths and manager

    """
    udf_manager = PythonUDFManager(project_dir=temp_udf_directory["project_dir"])

    yield {
        **temp_udf_directory,
        "udf_manager": udf_manager,
    }


@pytest.fixture(scope="function")
def error_prone_udf_file(temp_udf_directory: Dict[str, Any]) -> Path:
    """Create a UDF file with functions that demonstrate various error conditions.

    Args:
    ----
        temp_udf_directory: Temporary directory fixture

    Returns:
    -------
        Path to created error-prone UDF file

    """
    udf_file = Path(temp_udf_directory["udf_dir"]) / "error_prone_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            '''
"""UDFs that demonstrate various error conditions."""

import pandas as pd
from sqlflow.udfs import python_scalar_udf, python_table_udf


@python_scalar_udf
def divide_by_zero(value: float) -> float:
    """Attempt to divide by zero, which will raise a ZeroDivisionError."""
    return value / 0


@python_scalar_udf
def unsafe_string_operation(text: str) -> str:
    """Perform unsafe string operation that will raise a TypeError with None input."""
    if text is None:
        return text.upper()  # This will raise TypeError
    return text.upper()


@python_table_udf(output_schema={"result": "VARCHAR"})
def runtime_error_table_udf(df: pd.DataFrame) -> pd.DataFrame:
    """Table UDF that raises a runtime error."""
    raise RuntimeError("Simulated runtime error in table UDF")


@python_scalar_udf
def type_mismatch_udf(value: int) -> str:
    """UDF that expects int but might get string."""
    return str(value * 2)
'''
        )
    return udf_file


@pytest.fixture(scope="function")
def performance_udf_file(temp_udf_directory: Dict[str, Any]) -> Path:
    """Create a UDF file with performance test functions.

    Args:
    ----
        temp_udf_directory: Temporary directory fixture

    Returns:
    -------
        Path to created performance UDF file

    """
    udf_file = Path(temp_udf_directory["udf_dir"]) / "performance_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            '''
"""UDFs for performance testing."""

import pandas as pd
import numpy as np
from sqlflow.udfs import python_scalar_udf, python_table_udf


@python_scalar_udf
def simple_calculation(value: float) -> float:
    """Simple calculation for performance testing."""
    return value * 2 + 1


@python_scalar_udf
def complex_calculation(value: float) -> float:
    """More complex calculation for performance testing."""
    return np.sqrt(value ** 2 + np.sin(value) * np.cos(value))


@python_table_udf(output_schema={
    "id": "INTEGER",
    "processed_value": "DOUBLE",
    "category_rank": "INTEGER"
})
def vectorized_processing(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorized processing for performance testing."""
    result = df.copy()
    result["processed_value"] = result["value"] * 2 + np.random.random(len(df))
    result["category_rank"] = result.groupby("category")["value"].rank(method="dense").astype(int)
    return result[["id", "processed_value", "category_rank"]]
'''
        )
    return udf_file
