"""Decorators for defining Python User-Defined Functions (UDFs) in SQLFlow.

This module provides decorators for marking Python functions as SQLFlow UDFs,
which can then be discovered and used within SQL queries.
"""

import functools
from typing import Any, Callable, Optional, TypeVar, cast

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


def python_table_udf(
    func: Optional[FuncType] = None, *, name: Optional[str] = None
) -> Callable:
    """Decorator to mark a function as a SQLFlow table UDF.

    A table UDF processes an entire DataFrame and returns a DataFrame.

    Args:
        func: Python function that takes a DataFrame and returns a DataFrame
        name: Optional name for the UDF (defaults to the function name)

    Returns:
        The decorated function

    Example:
        @python_table_udf
        def add_metrics(df: pd.DataFrame) -> pd.DataFrame:
            result = df.copy()
            result['total'] = result['price'] * result['quantity']
            return result
    """

    def decorator(f: FuncType) -> FuncType:
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            result = f(*args, **kwargs)
            if not isinstance(result, pd.DataFrame):
                raise ValueError(
                    f"Table UDF {f.__name__} must return a pandas DataFrame, got {type(result)}"
                )
            return result

        # Mark function as a SQLFlow UDF
        wrapper._is_sqlflow_udf = True  # type: ignore
        wrapper._udf_type = "table"  # type: ignore
        wrapper._udf_name = name or f.__name__  # type: ignore

        return cast(FuncType, wrapper)

    # Handle both @decorator and @decorator(name="custom_name") syntax
    if func is None:
        return decorator
    return decorator(func)
