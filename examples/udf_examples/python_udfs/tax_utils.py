"""
Enhanced UDFs for SQLFlow demo.

This module contains UDFs with special handling for default parameters
to work around the DuckDB limitation with optional parameters.
"""

from sqlflow.udfs import python_scalar_udf


@python_scalar_udf
def calculate_tax_default(price: float) -> float:
    """Calculate the price with default tax rate (10%).

    This is a specialized version of calculate_tax that uses
    the default tax rate of 10% without requiring the parameter.

    Args:
        price: Original price

    Returns:
        Price with default tax (10%) added
    """
    if price is None:
        return None
    return price * 1.1  # Apply the default 10% tax
