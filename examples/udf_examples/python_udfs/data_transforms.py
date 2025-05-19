"""Data transformation UDFs for SQLFlow demo."""

import logging
from decimal import Decimal

import numpy as np
import pandas as pd

from sqlflow.udfs import python_scalar_udf, python_table_udf

# Set up logging
logger = logging.getLogger(__name__)


@python_scalar_udf
def calculate_tax(price: float, tax_rate: float = 0.1) -> float:
    """Calculate the price with tax added.

    Args:
        price: Original price
        tax_rate: Tax rate, default is 0.1 (10%)

    Returns:
        Price with tax added
    """
    if price is None:
        return None

    # Convert Decimal to float if needed
    price_float = float(price) if isinstance(price, Decimal) else price
    tax_rate_float = float(tax_rate) if isinstance(tax_rate, Decimal) else tax_rate

    return price_float * (1 + tax_rate_float)


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

    # Handle Decimal type directly here
    price_float = float(price) if isinstance(price, Decimal) else price

    return price_float * 1.1


@python_table_udf(
    output_schema={
        "price": "DOUBLE",
        "quantity": "INTEGER",
        "total": "DOUBLE",
        "tax": "DOUBLE",
        "final_price": "DOUBLE",
    }
)
def add_sales_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add sales metrics to a DataFrame.

    Args:
        df: Input DataFrame with 'price' and 'quantity' columns

    Returns:
        DataFrame with calculated metrics
    """
    result = df.copy()

    # Ensure 'price' column exists - use original_price if price doesn't exist
    if "price" not in result.columns and "original_price" in result.columns:
        result["price"] = result["original_price"]

    # Convert Decimal to float if needed
    if "price" in result.columns and result["price"].dtype == "object":
        result["price"] = result["price"].apply(
            lambda x: float(x) if isinstance(x, Decimal) else x
        )

    # Ensure all required columns exist
    if "price" not in result.columns:
        logger.warning("Column 'price' not found, adding empty column")
        result["price"] = 0.0

    if "quantity" not in result.columns:
        logger.warning("Column 'quantity' not found, adding empty column")
        result["quantity"] = 0

    # Calculate total
    result["total"] = result["price"] * result["quantity"]

    # Calculate tax at 10%
    result["tax"] = result["total"] * 0.1

    # Calculate final price
    result["final_price"] = result["total"] + result["tax"]

    return result


@python_table_udf(
    output_schema={
        # Input columns preserved in output
        "price": "DOUBLE",
        "quantity": "INTEGER",
        # Output columns with their types
        "z_score": "DOUBLE",
        "is_outlier": "BOOLEAN",
        "percentile": "DOUBLE",
    }
)
def detect_outliers(df: pd.DataFrame, column_name: str = "price") -> pd.DataFrame:
    """Detect outliers in a numeric column using Z-score method.

    Args:
        df: Input DataFrame
        column_name: Name of numeric column to check for outliers, default is "price"

    Returns:
        DataFrame with outlier detection flags
    """
    result = df.copy()

    if column_name in result.columns:
        # Calculate Z-scores for selected column
        mean = result[column_name].mean()
        std = result[column_name].std()

        if std > 0:  # Avoid division by zero
            result["z_score"] = (result[column_name] - mean) / std

            # Flag outliers (typically |z| > 3 indicates an outlier)
            result["is_outlier"] = np.abs(result["z_score"]) > 3

            # Add percentile information
            result["percentile"] = result[column_name].rank(pct=True) * 100
        else:
            # Handle case where std=0 (all values are identical)
            result["z_score"] = 0
            result["is_outlier"] = False
            result["percentile"] = 50  # All values at the same percentile
    else:
        # Add the required columns anyway to avoid schema mismatch
        result["z_score"] = None
        result["is_outlier"] = None
        result["percentile"] = None

    return result
