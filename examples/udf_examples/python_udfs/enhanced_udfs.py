"""
UDF utilities for SQLFlow.

This module provides decorators and utilities for creating and working with User-Defined Functions
in SQLFlow, specifically handling cases with default parameters.
"""

import functools
import inspect
from typing import Any, Callable, TypeVar

from sqlflow.udfs import python_scalar_udf

F = TypeVar("F", bound=Callable[..., Any])


def duckdb_compatible_udf(func: F) -> F:
    """
    Decorator that makes a function with default parameters compatible with DuckDB.

    This decorator creates specialized versions of the function for each parameter
    with a default value. These specialized versions can be called without the
    defaulted parameters.

    Args:
        func: The function to decorate

    Returns:
        Decorated function that has specialized versions available via attributes
    """
    # Original parameters and their default values
    sig = inspect.signature(func)
    func_name = func.__name__

    # Find parameters with default values
    default_params = {
        name: param.default
        for name, param in sig.parameters.items()
        if param.default is not inspect.Parameter.empty
    }

    # If no default parameters, just return the function
    if not default_params:
        return func

    # Add the specialized versions as attributes
    for param_name, default_value in default_params.items():
        # Create a specialized version that doesn't need this parameter
        specialized_name = f"without_{param_name}"

        # Define the specialized function
        @functools.wraps(func)
        def specialized_func(*args, **kwargs):
            # Add the default parameter if not provided
            if param_name not in kwargs:
                kwargs[param_name] = default_value
            return func(*args, **kwargs)

        # Set the name for debugging
        specialized_func.__name__ = f"{func_name}_{specialized_name}"

        # Apply the scalar UDF decorator to ensure it's properly registered
        decorated_specialized = python_scalar_udf(specialized_func)

        # Attach the specialized function as an attribute
        setattr(func, specialized_name, decorated_specialized)

    return func


# NOTE: This function is commented out to avoid conflicts with the calculate_tax
# function in data_transforms.py which creates duplicate UDF names
# @python_scalar_udf
# @duckdb_compatible_udf
# def calculate_tax(price: float, tax_rate: float = 0.1) -> float:
#     """Calculate the price with tax added.
#
#     Args:
#         price: Original price
#         tax_rate: Tax rate, default is 0.1 (10%)
#
#     Returns:
#         Price with tax added
#     """
#     if price is None:
#         return None
#     return price * (1 + tax_rate)
