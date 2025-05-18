"""Data transformation UDFs for SQLFlow demo."""

import numpy as np
import pandas as pd

from sqlflow.udfs import python_scalar_udf, python_table_udf


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
    return price * (1 + tax_rate)


@python_scalar_udf
def apply_discount(price: float, discount_percent: float) -> float:
    """Apply a percentage discount to a price.

    Args:
        price: Original price
        discount_percent: Discount percentage (0-100)

    Returns:
        Discounted price
    """
    if price is None:
        return None
    return price * (1 - discount_percent / 100)


@python_table_udf
def add_sales_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add sales metrics to a DataFrame.

    Args:
        df: Input DataFrame with 'price' and 'quantity' columns

    Returns:
        DataFrame with calculated metrics
    """
    result = df.copy()

    if "price" in result.columns and "quantity" in result.columns:
        # Calculate total
        result["total"] = result["price"] * result["quantity"]

        # Calculate tax at 10%
        result["tax"] = result["total"] * 0.1

        # Calculate final price
        result["final_price"] = result["total"] + result["tax"]

    return result


@python_table_udf
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

    return result
