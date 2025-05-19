"""Decorators for defining Python User-Defined Functions (UDFs) in SQLFlow.

This module provides decorators for marking Python functions as SQLFlow UDFs,
which can then be discovered and used within SQL queries.
"""

import functools
import inspect
from typing import Any, Callable, Dict, List, Optional, TypeVar, cast

import pandas as pd

FuncType = TypeVar("FuncType", bound=Callable[..., Any])


def python_scalar_udf(
    func: Optional[FuncType] = None, *, name: Optional[str] = None
) -> Callable:
    """Decorator to mark a function as a SQLFlow scalar UDF.

    A scalar UDF processes one row at a time and returns a single value.

    Args:
        func: Python function to register as a UDF
        name: Optional name for the UDF (defaults to the function name)

    Returns:
        The decorated function

    Example:
        @python_scalar_udf
        def add_tax(price: float, tax_rate: float = 0.1) -> float:
            return price * (1 + tax_rate)
    """

    def decorator(f: FuncType) -> FuncType:
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)

        # Mark function as a SQLFlow UDF
        wrapper._is_sqlflow_udf = True  # type: ignore
        wrapper._udf_type = "scalar"  # type: ignore
        wrapper._udf_name = name or f.__name__  # type: ignore

        return cast(FuncType, wrapper)

    # Handle both @decorator and @decorator(name="custom_name") syntax
    if func is None:
        return decorator
    return decorator(func)


def _validate_table_udf_signature(
    func: Callable, params: List[inspect.Parameter]
) -> None:
    """Validate the signature of a table UDF function.

    Args:
        func: The function to validate
        params: List of function parameters

    Raises:
        ValueError: If the signature is invalid
    """
    if not params:
        raise ValueError(
            f"Table UDF {func.__name__} must accept at least one argument (DataFrame)"
        )

    # First parameter must be positional or positional_or_keyword
    first_param = params[0]
    if first_param.kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise ValueError(
            f"First parameter of table UDF {func.__name__} must be positional "
            "(DataFrame argument)"
        )

    # Remaining parameters must be keyword arguments
    for param in params[1:]:
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.VAR_POSITIONAL,
        ):
            raise ValueError(
                f"Additional parameters in table UDF {func.__name__} must be keyword "
                f"arguments, got {param.name} as {param.kind}"
            )


def _validate_table_udf_input(
    func: Callable, df: Any, required_columns: Optional[List[str]] = None
) -> None:
    """Validate the input to a table UDF function.

    Args:
        func: The UDF function
        df: The input DataFrame
        required_columns: Optional list of required column names

    Raises:
        ValueError: If the input is invalid
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError(
            f"First argument to table UDF {func.__name__} must be a DataFrame, "
            f"got {type(df)}"
        )

    if required_columns:
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Table UDF {func.__name__} requires columns that are missing from "
                f"input DataFrame: {missing_cols}"
            )


def _validate_table_udf_output(
    func: Callable, result: Any, output_schema: Optional[Dict[str, str]] = None
) -> None:
    """Validate the output of a table UDF function.

    Args:
        func: The UDF function
        result: The function result to validate
        output_schema: Expected output schema

    Raises:
        ValueError: If the output is invalid
    """
    if not isinstance(result, pd.DataFrame):
        raise ValueError(
            f"Table UDF {func.__name__} must return a pandas DataFrame, "
            f"got {type(result)}"
        )

    if output_schema:
        # Validate result DataFrame has all the expected columns
        missing_cols = [col for col in output_schema if col not in result.columns]
        if missing_cols:
            raise ValueError(
                f"Table UDF {func.__name__} output is missing expected columns: {missing_cols}"
            )

        # Check for type compatibility where possible
        for col_name, expected_type in output_schema.items():
            if col_name in result.columns:
                # Map pandas dtype to Python type name for comparison
                # This is a simplified version; a more robust implementation would
                # use a comprehensive mapping of pandas dtypes to SQL types
                dtype_map = {
                    "int64": ["int", "integer", "int64", "bigint"],
                    "float64": ["float", "double", "float64", "numeric", "decimal"],
                    "bool": ["bool", "boolean"],
                    "object": ["str", "string", "varchar", "text", "char"],
                }

                actual_dtype = str(result[col_name].dtype).lower()
                expected_type_lower = expected_type.lower()

                # Check if the actual type is compatible with the expected type
                type_match = False
                for dtype, compatible_types in dtype_map.items():
                    if actual_dtype.startswith(dtype) and any(
                        expected_type_lower.startswith(t) for t in compatible_types
                    ):
                        type_match = True
                        break

                if not type_match:
                    # This is a warning rather than an error for flexibility
                    import warnings

                    warnings.warn(
                        f"Column '{col_name}' in {func.__name__} output has type '{actual_dtype}', "
                        f"which may not be compatible with expected type '{expected_type}'"
                    )


def _create_param_info(
    params: Dict[str, inspect.Parameter],
) -> Dict[str, Dict[str, Any]]:
    """Create parameter information dictionary for UDF metadata.

    Args:
        params: Dictionary of parameter names to Parameter objects

    Returns:
        Dictionary containing parameter metadata
    """
    return {
        name: {
            "kind": param.kind,
            "default": (
                None if param.default is inspect.Parameter.empty else param.default
            ),
            "annotation": (
                "Any"
                if param.annotation is inspect.Parameter.empty
                else str(param.annotation)
            ),
        }
        for name, param in params.items()
    }


def _infer_output_schema(df: pd.DataFrame) -> Dict[str, str]:
    """Infer output schema from a pandas DataFrame.

    Args:
        df: DataFrame to infer schema from

    Returns:
        Dictionary of column names to inferred types
    """
    # Map pandas dtypes to SQL-like type names
    dtype_to_sql = {
        "int64": "INTEGER",
        "int32": "INTEGER",
        "float64": "DOUBLE",
        "float32": "FLOAT",
        "bool": "BOOLEAN",
        "object": "VARCHAR",  # Usually strings
        "datetime64": "TIMESTAMP",
        "category": "VARCHAR",
    }

    schema = {}
    for col_name, dtype in df.dtypes.items():
        dtype_str = str(dtype)
        # Handle special cases like datetime with timezone
        if "datetime" in dtype_str:
            schema[col_name] = "TIMESTAMP"
        else:
            # Get the base dtype name
            base_dtype = dtype_str.split("[")[0]
            schema[col_name] = dtype_to_sql.get(base_dtype, "VARCHAR")

    return schema


def python_table_udf(
    func: Optional[FuncType] = None,
    *,
    name: Optional[str] = None,
    required_columns: Optional[List[str]] = None,
    output_schema: Optional[Dict[str, str]] = None,
    infer: bool = False,
) -> Callable:
    """Decorator to mark a function as a SQLFlow table UDF.

    A table UDF processes an entire DataFrame and returns a DataFrame. The function
    must accept a pandas DataFrame as its first argument, followed by optional
    keyword arguments.

    Args:
        func: Python function that takes a DataFrame and returns a DataFrame
        name: Optional name for the UDF (defaults to the function name)
        required_columns: Optional list of column names that must be present in input DataFrame
        output_schema: Required dictionary mapping output column names to their types
                      (e.g., {"name": "VARCHAR", "age": "INTEGER"})
        infer: If True, automatically infer the output schema from the first execution
              Note: This should only be used for development or quick prototypes

    Returns:
        The decorated function

    Example:
        @python_table_udf(
            required_columns=["price", "quantity"],
            output_schema={"price": "DOUBLE", "quantity": "INTEGER", "total": "DOUBLE"}
        )
        def add_metrics(df: pd.DataFrame, tax_rate: float = 0.1) -> pd.DataFrame:
            result = df.copy()
            result["total"] = result["price"] * result["quantity"]
            result["tax"] = result["total"] * tax_rate
            return result

    Raises:
        ValueError: If the function signature is invalid, required columns are missing,
                   or output schema validation fails
    """

    def decorator(f: FuncType) -> FuncType:
        # Validate function signature
        sig = inspect.signature(f)
        params = list(sig.parameters.values())
        _validate_table_udf_signature(f, params)

        # Validate that either output_schema is provided or infer=True
        if output_schema is None and not infer:
            raise ValueError(
                f"Table UDF {f.__name__} must specify output_schema or set infer=True. "
                f"Using infer=True is recommended only for development."
            )

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            if not args:
                raise ValueError(
                    f"Table UDF {f.__name__} requires a DataFrame argument"
                )

            df = args[0]
            _validate_table_udf_input(f, df, required_columns)

            # Call the function and validate return type
            result = f(df, **kwargs)

            # Infer schema if needed (first execution only)
            nonlocal output_schema
            if output_schema is None and infer:
                output_schema = _infer_output_schema(result)
                wrapper._output_schema = output_schema  # type: ignore

            _validate_table_udf_output(f, result, output_schema)

            return result

        # Mark function as a SQLFlow UDF with enhanced metadata
        wrapper._is_sqlflow_udf = True  # type: ignore
        wrapper._udf_type = "table"  # type: ignore
        wrapper._udf_name = name or f.__name__  # type: ignore
        wrapper._required_columns = required_columns  # type: ignore
        wrapper._signature = str(sig)  # type: ignore
        wrapper._param_info = _create_param_info(sig.parameters)  # type: ignore
        wrapper._output_schema = output_schema  # type: ignore
        wrapper._infer_schema = infer  # type: ignore

        return cast(FuncType, wrapper)

    # Handle both @decorator and @decorator(name="custom_name") syntax
    if func is None:
        return decorator
    return decorator(func)
