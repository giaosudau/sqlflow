"""Comprehensive UDF error handling and edge case tests.

This module consolidates all UDF error handling scenarios:
- Runtime errors (division by zero, type errors, attribute errors)
- UDF registration and validation errors
- Edge cases (null data, empty datasets, invalid schemas)
- Complex error propagation in pipelines
- Production-like error scenarios

Tests follow naming convention: test_{error_type}_{scenario}
Each test represents a real error condition users encounter.
"""

import math
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Generator

import numpy as np
import pandas as pd
import pytest

from sqlflow.core.engines.duckdb import DuckDBEngine
from sqlflow.logging import get_logger
from sqlflow.udfs.decorators import python_table_udf
from sqlflow.udfs.manager import PythonUDFManager

logger = get_logger(__name__)


@pytest.fixture
def error_handling_test_env() -> Generator[Dict[str, Any], None, None]:
    """Create test environment with error-prone UDF functions."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        udf_dir = os.path.join(tmp_dir, "python_udfs")
        os.makedirs(udf_dir, exist_ok=True)

        # Create error-prone UDF file
        error_udf_file = create_error_prone_udf_file(udf_dir)

        # Create edge case UDF file
        edge_case_file = create_edge_case_udf_file(udf_dir)

        yield {
            "project_dir": tmp_dir,
            "udf_dir": udf_dir,
            "error_udf_file": error_udf_file,
            "edge_case_file": edge_case_file,
        }


def create_error_prone_udf_file(udf_dir: str) -> Path:
    """Create UDF file with functions that raise various errors."""
    udf_file = Path(udf_dir) / "error_prone_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            '''
"""UDFs that demonstrate various error conditions for testing."""

import math
import pandas as pd
from decimal import Decimal
from typing import Any
from sqlflow.udfs import python_scalar_udf, python_table_udf


@python_scalar_udf
def divide_by_zero(value: float) -> float:
    """UDF that raises ZeroDivisionError."""
    return value / 0


@python_scalar_udf  
def unsafe_string_operation(text: str) -> str:
    """UDF that raises TypeError with None input."""
    if text is None:
        return text.upper()  # TypeError: 'NoneType' has no attribute 'upper'
    return text.upper()


@python_scalar_udf
def attribute_error_func(obj: Any) -> str:
    """UDF that raises AttributeError."""
    return obj.non_existent_attribute


@python_scalar_udf
def math_domain_error(value: float) -> float:
    """UDF that raises ValueError for invalid math operations."""
    return math.log(value)  # Fails for value <= 0


@python_scalar_udf
def type_conversion_error(value: Any) -> float:
    """UDF that raises TypeError for incompatible conversions."""
    return float(value) + 10.0  # Fails for non-numeric strings


@python_scalar_udf  
def decimal_handling_error(amount: float, rate: float) -> float:
    """UDF with potential Decimal conversion issues."""
    # Intentionally don't handle Decimal types
    return amount * rate  # May fail if DuckDB passes Decimal objects


@python_table_udf(output_schema={
    "id": "INTEGER",
    "processed_value": "DOUBLE"
})
def missing_column_error(df: pd.DataFrame) -> pd.DataFrame:
    """Table UDF that raises KeyError for missing column."""
    result = pd.DataFrame()
    result["id"] = df["id"] if "id" in df.columns else pd.Series(dtype="Int64")
    result["processed_value"] = df["non_existent_column"] * 2  # KeyError
    return result


@python_table_udf(output_schema={
    "id": "INTEGER", 
    "value": "DOUBLE",
    "description": "VARCHAR"
})
def index_out_of_bounds_error(df: pd.DataFrame) -> pd.DataFrame:
    """Table UDF that raises IndexError."""
    result = pd.DataFrame()
    result["id"] = df["id"] if "id" in df.columns else pd.Series(dtype="Int64")
    result["value"] = df["value"] if "value" in df.columns else pd.Series(dtype="float64")
    
    descriptions = ["Low", "Medium", "High"]
    # IndexError for values >= 3
    result["description"] = df["value"].apply(lambda x: descriptions[int(x)])
    return result


@python_table_udf(output_schema={
    "id": "INTEGER",
    "value": "DOUBLE", 
    "status": "VARCHAR"
})
def schema_mismatch_error(df: pd.DataFrame) -> pd.DataFrame:
    """Table UDF that returns wrong schema."""
    result = pd.DataFrame()
    result["id"] = df["id"] if "id" in df.columns else pd.Series(dtype="Int64")
    result["value"] = df["value"] if "value" in df.columns else pd.Series(dtype="float64")
    result["wrong_column"] = "Wrong"  # Returns 'wrong_column' instead of 'status'
    return result


@python_table_udf(output_schema={
    "id": "INTEGER",
    "result": "VARCHAR"
})
def wrong_return_type_error(df: pd.DataFrame) -> str:
    """Table UDF that returns wrong type."""
    return "Should return DataFrame, not string"


@python_table_udf(output_schema={
    "result": "VARCHAR"
})
def empty_input_error(df: pd.DataFrame) -> pd.DataFrame:
    """Table UDF that fails with empty input."""
    if df.empty:
        raise ValueError("Cannot process empty DataFrame")
    return pd.DataFrame({"result": ["processed"]})


@python_scalar_udf
def unsafe_log(value: float) -> float:
    """UDF that fails for negative or zero values."""
    return math.log(value)  # Will fail for value <= 0


@python_table_udf(output_schema={
    "id": "INTEGER",
    "safe_result": "DOUBLE",
    "status": "VARCHAR"
})
def unsafe_table_processing(df: pd.DataFrame) -> pd.DataFrame:
    """Table UDF that fails when processing negative values."""
    result = pd.DataFrame()
    result["id"] = df["id"]
    
    # This will fail for negative values
    result["safe_result"] = df["value"].apply(lambda x: math.sqrt(x) if x >= 0 else math.sqrt(x))
    result["status"] = "processed"
    
    return result[["id", "safe_result", "status"]]
'''
        )
    return udf_file


def create_edge_case_udf_file(udf_dir: str) -> Path:
    """Create UDF file with edge case handling functions."""
    udf_file = Path(udf_dir) / "edge_case_udfs.py"
    with open(udf_file, "w") as f:
        f.write(
            '''
"""UDFs that handle edge cases properly for testing."""

import math
import pandas as pd
import numpy as np
from decimal import Decimal
from sqlflow.udfs import python_scalar_udf, python_table_udf


@python_scalar_udf
def safe_divide(numerator: float, denominator: float) -> float:
    """Safely divide with zero handling."""
    if denominator == 0:
        return float('inf') if numerator > 0 else float('-inf') if numerator < 0 else float('nan')
    return numerator / denominator


@python_scalar_udf
def safe_log(value: float) -> float:
    """Safely compute logarithm with domain validation."""
    if value is None or value <= 0:
        return float('nan')  # Return NaN instead of None
    return math.log(value)


@python_scalar_udf
def decimal_safe_multiply(amount: float, rate: float = 0.1) -> float:
    """Multiply with Decimal conversion safety."""
    amount_float = float(amount) if isinstance(amount, Decimal) else amount
    rate_float = float(rate) if isinstance(rate, Decimal) else rate
    return amount_float * rate_float


@python_table_udf(output_schema={
    "id": "INTEGER",
    "safe_result": "DOUBLE",
    "status": "VARCHAR"
})
def robust_processing(df: pd.DataFrame) -> pd.DataFrame:
    """Table UDF with comprehensive error handling."""
    result = df.copy()
    
    def safe_calculation(row):
        try:
            value = pd.to_numeric(row.get("value", 0), errors="coerce")
            if pd.isna(value) or value == 0:
                return 0.0, "zero_or_null"
            elif value < 0:
                return abs(value), "negative_handled"
            else:
                return np.sqrt(value) * 2, "success"
        except Exception:
            return 0.0, "error_handled"
    
    calculations = result.apply(safe_calculation, axis=1, result_type="expand")
    result["safe_result"] = calculations[0]
    result["status"] = calculations[1]
    
    return result[["id", "safe_result", "status"]]


@python_table_udf(output_schema={
    "id": "INTEGER",
    "processed": "VARCHAR", 
    "is_valid": "BOOLEAN"
})
def null_safe_processing(df: pd.DataFrame) -> pd.DataFrame:
    """Table UDF that safely handles NULL and empty data."""
    if df.empty:
        return pd.DataFrame({
            "id": pd.Series(dtype="Int64"),
            "processed": pd.Series(dtype="object"),
            "is_valid": pd.Series(dtype="boolean")
        })
    
    result = df.copy()
    result["processed"] = result.get("text", pd.Series(dtype=str)).fillna("NULL_VALUE")
    result["is_valid"] = ~result.get("text", pd.Series(dtype=str)).isna()
    
    return result[["id", "processed", "is_valid"]]


@python_table_udf(output_schema={
    "customer_id": "INTEGER",
    "normalized_name": "VARCHAR",
    "validation_status": "VARCHAR"
})
def schema_validation_processing(df: pd.DataFrame) -> pd.DataFrame:
    """Table UDF with schema validation."""
    required_columns = ["customer_id", "customer_name"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return pd.DataFrame({
            "customer_id": [0],
            "normalized_name": [""],
            "validation_status": [f"Missing columns: {', '.join(missing_columns)}"]
        })
    
    result = pd.DataFrame()
    result["customer_id"] = df["customer_id"]
    result["normalized_name"] = df["customer_name"].str.strip().str.upper()
    result["validation_status"] = "valid"
    
    return result
'''
        )
    return udf_file


class TestRuntimeErrors:
    """Test runtime errors in UDF execution."""

    def test_runtime_error_division_by_zero(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User encounters division by zero error in scalar UDF."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        manager.discover_udfs()

        engine = DuckDBEngine(":memory:")
        manager.register_udfs_with_engine(engine)

        # Create test data
        engine.execute_query(
            """
            CREATE TABLE test_data AS
            SELECT * FROM VALUES (1), (2), (3) AS t(value)
        """
        )

        # Test division by zero error
        with pytest.raises(Exception) as excinfo:
            engine.execute_query(
                """
                SELECT value, divide_by_zero(value) as result 
                FROM test_data
            """
            )

        error_message = str(excinfo.value)
        assert (
            "divide_by_zero" in error_message.lower()
            or "division by zero" in error_message.lower()
        )

    def test_runtime_error_type_error(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User encounters type error with None values in UDF."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        manager.discover_udfs()

        engine = DuckDBEngine(":memory:")
        manager.register_udfs_with_engine(engine)

        # Test with NULL value - DuckDB should handle this gracefully
        result = engine.execute_query(
            """
            SELECT 
                'hello' as text,
                unsafe_string_operation('hello') as result_1,
                unsafe_string_operation(NULL) as result_2
        """
        ).fetchdf()

        assert result.iloc[0]["result_1"] == "HELLO"
        assert pd.isna(result.iloc[0]["result_2"])  # NULL should result in NULL

    def test_runtime_error_math_domain(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User encounters math domain error with invalid values."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        manager.discover_udfs()

        engine = DuckDBEngine(":memory:")
        manager.register_udfs_with_engine(engine)

        # Create test data with negative values
        engine.execute_query(
            """
            CREATE TABLE test_data AS
            SELECT * FROM VALUES (10.0), (-5.0), (0.0) AS t(value)
        """
        )

        # Test math domain error with negative values
        with pytest.raises(Exception) as excinfo:
            engine.execute_query(
                """
                SELECT value, math_domain_error(value) as log_result 
                FROM test_data
            """
            )

        error_message = str(excinfo.value)
        assert (
            "math_domain_error" in error_message.lower()
            or "domain error" in error_message.lower()
        )

    def test_runtime_error_type_conversion(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User encounters type conversion error with incompatible types."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        manager.discover_udfs()

        engine = DuckDBEngine(":memory:")
        manager.register_udfs_with_engine(engine)

        # Test type conversion with non-numeric string
        with pytest.raises(Exception) as excinfo:
            engine.execute_query(
                """
                SELECT type_conversion_error('not_a_number') as result
            """
            )

        error_message = str(excinfo.value)
        assert (
            "type_conversion_error" in error_message.lower()
            or "invalid" in error_message.lower()
        )

    def test_runtime_error_decimal_handling(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User encounters Decimal type handling errors."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        manager.discover_udfs()

        engine = DuckDBEngine(":memory:")
        manager.register_udfs_with_engine(engine)

        # Test with decimal values that may cause type errors
        try:
            result = engine.execute_query(
                """
                SELECT decimal_handling_error(100.50, 0.08) as result
            """
            ).fetchdf()
            # If it works, that's fine - DuckDB might convert automatically
            assert result.iloc[0]["result"] == pytest.approx(8.04, rel=1e-6)
        except Exception as excinfo:
            # If it fails, verify it's a type-related error
            error_message = str(excinfo)
            assert "decimal" in error_message.lower() or "type" in error_message.lower()


class TestTableUDFErrors:
    """Test errors specific to table UDFs."""

    def test_table_udf_missing_column_error(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User encounters KeyError from missing column in table UDF."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Get the table UDF function directly
        missing_column_func = udfs["python_udfs.error_prone_udfs.missing_column_error"]

        # Test DataFrame with missing column
        test_df = pd.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})

        with pytest.raises(KeyError) as excinfo:
            missing_column_func(test_df)

        error_message = str(excinfo.value)
        assert "non_existent_column" in error_message

    def test_table_udf_index_error(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User encounters IndexError in table UDF processing."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Get the table UDF function directly
        index_error_func = udfs[
            "python_udfs.error_prone_udfs.index_out_of_bounds_error"
        ]

        # Test DataFrame with values that cause index error
        test_df = pd.DataFrame(
            {"id": [1, 2, 3, 4], "value": [0, 1, 2, 5]}
        )  # 5 > 2 (array size)

        with pytest.raises(IndexError) as excinfo:
            index_error_func(test_df)

        error_message = str(excinfo.value)
        assert (
            "index" in error_message.lower() or "out of range" in error_message.lower()
        )

    def test_table_udf_schema_mismatch_error(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User encounters schema mismatch in table UDF output."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Get the table UDF function directly
        schema_mismatch_func = udfs[
            "python_udfs.error_prone_udfs.schema_mismatch_error"
        ]

        # Test DataFrame
        test_df = pd.DataFrame({"id": [1, 2], "value": [10.0, 20.0]})

        # This should work but return wrong columns
        with pytest.raises(ValueError) as excinfo:
            schema_mismatch_func(test_df)

        error_message = str(excinfo.value)
        assert (
            "missing expected columns" in error_message.lower()
            or "status" in error_message.lower()
        )

    def test_table_udf_wrong_return_type_error(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User encounters wrong return type error in table UDF."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Get the table UDF function directly
        wrong_type_func = udfs["python_udfs.error_prone_udfs.wrong_return_type_error"]

        # Test DataFrame
        test_df = pd.DataFrame({"id": [1, 2], "value": [10.0, 20.0]})

        with pytest.raises(ValueError) as excinfo:
            wrong_type_func(test_df)

        error_message = str(excinfo.value)
        assert "must return a pandas DataFrame" in error_message


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_edge_case_null_data_handling(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User processes data with NULL values using robust UDFs."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Get the null-safe UDF function
        null_safe_func = udfs["python_udfs.edge_case_udfs.null_safe_processing"]

        # Test with NULL values
        test_df = pd.DataFrame(
            {"id": [1, 2, 3, 4], "text": ["hello", None, "world", None]}
        )

        result = null_safe_func(test_df)

        assert len(result) == 4
        assert result.iloc[1]["processed"] == "NULL_VALUE"
        assert not result.iloc[1]["is_valid"]
        assert result.iloc[0]["is_valid"]

    def test_edge_case_empty_dataset_handling(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User processes empty datasets with robust UDFs."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Get the null-safe UDF function
        null_safe_func = udfs["python_udfs.edge_case_udfs.null_safe_processing"]

        # Test with empty dataset
        empty_df = pd.DataFrame({"id": [], "text": []})

        result = null_safe_func(empty_df)

        assert len(result) == 0
        assert list(result.columns) == ["id", "processed", "is_valid"]

    def test_edge_case_robust_error_handling(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User processes data with various edge cases using robust UDF."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Get the robust processing UDF function
        robust_func = udfs["python_udfs.edge_case_udfs.robust_processing"]

        # Test with edge case data
        edge_case_df = pd.DataFrame(
            {"id": [1, 2, 3, 4, 5, 6], "value": [100, -50, 0, None, "invalid", np.inf]}
        )

        result = robust_func(edge_case_df)

        assert len(result) == 6
        assert "safe_result" in result.columns
        assert "status" in result.columns

        # Check specific edge case handling
        status_counts = result.status.value_counts()
        assert "zero_or_null" in status_counts.index
        assert "negative_handled" in status_counts.index
        assert "success" in status_counts.index

    def test_edge_case_safe_math_operations(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User performs safe math operations that handle edge cases."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        manager.discover_udfs()

        engine = DuckDBEngine(":memory:")
        manager.register_udfs_with_engine(engine)

        # Test safe division
        result = engine.execute_query(
            """
            SELECT 
                safe_divide(10, 2) as normal_div,
                safe_divide(10, 0) as div_by_zero,
                safe_divide(-10, 0) as neg_div_by_zero,
                safe_divide(0, 0) as zero_div_zero
        """
        ).fetchdf()

        assert result.iloc[0]["normal_div"] == 5.0
        assert result.iloc[0]["div_by_zero"] == float("inf")
        assert result.iloc[0]["neg_div_by_zero"] == float("-inf")
        assert pd.isna(result.iloc[0]["zero_div_zero"])

        # Test safe logarithm
        result = engine.execute_query(
            """
            SELECT 
                safe_log(10) as normal_log,
                safe_log(0) as log_zero,
                safe_log(-5) as log_negative,
                safe_log(NULL) as log_null
        """
        ).fetchdf()

        assert result.iloc[0]["normal_log"] == pytest.approx(math.log(10), rel=1e-6)
        assert math.isnan(result.iloc[0]["log_zero"])  # log(0) -> NaN
        assert math.isnan(result.iloc[0]["log_negative"])  # log(-5) -> NaN
        assert math.isnan(result.iloc[0]["log_null"])  # log(NULL) -> NaN


class TestValidationErrors:
    """Test UDF validation and registration errors."""

    def test_validation_error_schema_mismatch(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User encounters schema validation errors in table UDFs."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        udfs = manager.discover_udfs()

        # Get the schema validation UDF function
        schema_validation_func = udfs[
            "python_udfs.edge_case_udfs.schema_validation_processing"
        ]

        # Test with valid schema
        valid_df = pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "customer_name": ["  john doe  ", "Jane Smith", "bob wilson"],
            }
        )

        result = schema_validation_func(valid_df)

        assert len(result) == 3
        assert all(result["validation_status"] == "valid")
        assert result.iloc[0]["normalized_name"] == "JOHN DOE"

        # Test with invalid schema
        invalid_df = pd.DataFrame(
            {"customer_id": [1, 2], "wrong_column": ["value1", "value2"]}
        )

        result = schema_validation_func(invalid_df)

        assert len(result) == 1
        assert "Missing columns" in result.iloc[0]["validation_status"]

    def test_validation_error_invalid_udf_signature(self) -> None:
        """User creates UDF with invalid signature."""
        with pytest.raises(ValueError) as excinfo:

            @python_table_udf(output_schema={"result": "VARCHAR"})
            def invalid_signature_udf():  # Missing DataFrame parameter
                return pd.DataFrame({"result": ["test"]})

        error_message = str(excinfo.value)
        assert "must accept at least one argument" in error_message

    def test_validation_error_missing_output_schema(self) -> None:
        """User creates table UDF without required output schema."""
        with pytest.raises(ValueError) as excinfo:

            @python_table_udf  # Missing output_schema and infer=True
            def missing_schema_udf(df: pd.DataFrame) -> pd.DataFrame:
                return df

        error_message = str(excinfo.value)
        assert "must specify output_schema or set infer=True" in error_message


class TestComplexErrorPropagation:
    """Test error propagation in complex scenarios."""

    def test_complex_error_propagation_in_pipeline(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User encounters errors that propagate through multiple pipeline steps."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        manager.discover_udfs()

        engine = DuckDBEngine(":memory:")
        manager.register_udfs_with_engine(engine)

        # Create test data with problematic values
        engine.execute_query(
            """
            CREATE TABLE pipeline_input AS
            SELECT * FROM VALUES
                (1, 10.0, 'A'),
                (2, 0.0, 'B'),    -- Will cause math_domain_error
                (3, -5.0, 'C'),   -- Will cause math_domain_error
                (4, 100.0, 'D')
            AS t(id, value, category)
        """
        )

        # Multi-step pipeline that should fail
        with pytest.raises(Exception) as excinfo:
            engine.execute_query(
                """
                WITH step1 AS (
                    SELECT * FROM pipeline_input WHERE id > 0
                ),
                step2 AS (
                    SELECT 
                        id,
                        value, 
                        category,
                        math_domain_error(value) as log_value
                    FROM step1
                )
                SELECT 
                    id,
                    category,
                    log_value,
                    log_value * 2 as doubled_log
                FROM step2
            """
            )

        error_message = str(excinfo.value)
        assert (
            "math_domain_error" in error_message.lower()
            or "domain error" in error_message.lower()
        )

    def test_complex_error_recovery_patterns(
        self, error_handling_test_env: Dict[str, Any]
    ) -> None:
        """User implements error recovery patterns in complex queries."""
        manager = PythonUDFManager(project_dir=error_handling_test_env["project_dir"])
        manager.discover_udfs()

        engine = DuckDBEngine(":memory:")
        manager.register_udfs_with_engine(engine)

        # Create test data
        engine.execute_query(
            """
            CREATE TABLE recovery_test AS
            SELECT * FROM VALUES
                (1, 10.0),
                (2, 0.0),
                (3, -5.0),
                (4, 100.0)
            AS t(id, value)
        """
        )

        # Use safe UDFs for error recovery
        result = engine.execute_query(
            """
            SELECT 
                id,
                value,
                safe_log(value) as safe_log_result,
                safe_divide(value, 2) as safe_div_result,
                decimal_safe_multiply(value, 0.1) as safe_mult_result
            FROM recovery_test
            ORDER BY id
        """
        ).fetchdf()

        assert len(result) == 4

        # Check safe handling
        assert result.iloc[0]["safe_log_result"] == pytest.approx(
            math.log(10), rel=1e-6
        )
        assert math.isnan(result.iloc[1]["safe_log_result"])  # log(0) -> NaN
        assert math.isnan(result.iloc[2]["safe_log_result"])  # log(-5) -> NaN

        # Safe division should work for all
        assert result.iloc[0]["safe_div_result"] == 5.0
        assert result.iloc[1]["safe_div_result"] == 0.0
        assert result.iloc[2]["safe_div_result"] == -2.5

    def test_complex_error_mixed_udf_types(
        self,
        error_handling_test_env: Dict[str, Any],
        v2_pipeline_runner,
        tmp_path: Path,
    ) -> None:
        """User has a pipeline with both scalar and table UDFs where one fails."""
        project_dir = error_handling_test_env["project_dir"]
        test_data = pd.DataFrame({"id": [1, 2], "value": [1.0, -1.0]})
        source_path = tmp_path / "source_data.csv"
        test_data.to_csv(source_path, index=False)

        pipeline = {
            "steps": [
                {
                    "type": "load",
                    "name": "source_data",
                    "source": str(source_path),
                },
                {
                    "type": "transform",
                    "name": "processed",
                    "query": 'SELECT * FROM PYTHON_FUNC("python_udfs.error_prone_udfs.unsafe_table_processing", source_data)',
                },
                {
                    "type": "transform",
                    "name": "final_result",
                    "query": "SELECT python_udfs.error_prone_udfs.unsafe_log(safe_result) FROM processed",
                },
            ]
        }

        coordinator = v2_pipeline_runner(pipeline["steps"], project_dir=project_dir)
        result = coordinator.result
        assert not result.success, "Pipeline should fail due to error in UDF"

        # Verify error propagation - pipeline should fail at any step
        failed_steps = [step for step in result.step_results if not step.success]
        assert len(failed_steps) > 0, "Should have at least one failed step"

        # The failure could be at any step due to UDF registration issues
        failed_step = failed_steps[0]
        assert failed_step.error_message is not None
        assert any(
            keyword in str(failed_step.error_message).lower()
            for keyword in ["python_func", "catalog", "udf", "log"]
        )
