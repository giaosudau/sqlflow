"""Alternative approaches to table UDFs that work with DuckDB limitations.

Since DuckDB Python API doesn't support true table functions, this module demonstrates
several alternative patterns that achieve similar results.
"""

import logging
from decimal import Decimal
from typing import Optional

import numpy as np
import pandas as pd

from sqlflow.udfs import python_scalar_udf

logger = logging.getLogger(__name__)


# ===== SCALAR UDF APPROACH =====
# Use scalar UDFs to add calculated columns one by one


@python_scalar_udf
def calculate_sales_total(price: float, quantity: int) -> Optional[float]:
    """Calculate total for a sale (price * quantity)."""
    if price is None or quantity is None:
        return None

    price_float = float(price) if isinstance(price, Decimal) else price
    return price_float * quantity


@python_scalar_udf
def calculate_sales_tax(total: float, tax_rate: float = 0.1) -> Optional[float]:
    """Calculate tax on a total amount."""
    if total is None:
        return None

    total_float = float(total) if isinstance(total, Decimal) else total
    return total_float * tax_rate


@python_scalar_udf
def calculate_final_price(total: float, tax: float) -> Optional[float]:
    """Calculate final price including tax."""
    if total is None or tax is None:
        return None

    total_float = float(total) if isinstance(total, Decimal) else total
    tax_float = float(tax) if isinstance(tax, Decimal) else tax
    return total_float + tax_float


@python_scalar_udf
def calculate_z_score(
    value: float, mean_value: float, std_value: float
) -> Optional[float]:
    """Calculate Z-score for outlier detection."""
    if value is None or mean_value is None or std_value is None or std_value == 0:
        return None

    value_float = float(value) if isinstance(value, Decimal) else value
    mean_float = float(mean_value) if isinstance(mean_value, Decimal) else mean_value
    std_float = float(std_value) if isinstance(std_value, Decimal) else std_value

    return (value_float - mean_float) / std_float


@python_scalar_udf
def is_outlier(z_score: float, threshold: float = 3.0) -> bool:
    """Determine if a value is an outlier based on Z-score."""
    if z_score is None:
        return False

    z_float = float(z_score) if isinstance(z_score, Decimal) else z_score
    return abs(z_float) > threshold


@python_scalar_udf
def calculate_percentile_rank(
    value: float, total_count: int, rank_position: int
) -> Optional[float]:
    """Calculate percentile rank for a value."""
    if (
        value is None
        or total_count is None
        or rank_position is None
        or total_count == 0
    ):
        return None

    return (rank_position / total_count) * 100


# ===== HELPER FUNCTIONS FOR EXTERNAL PROCESSING =====
# These functions can be used in pipeline preprocessing


def add_sales_metrics_external(df: pd.DataFrame) -> pd.DataFrame:
    """Add sales metrics to a DataFrame using external processing.

    This function can be called before registering the DataFrame with DuckDB.

    Args:
    ----
        df: Input DataFrame with 'price' and 'quantity' columns

    Returns:
    -------
        DataFrame with calculated metrics

    """
    result = df.copy()

    # Ensure 'price' column exists
    if "price" not in result.columns and "original_price" in result.columns:
        result["price"] = result["original_price"]

    # Convert Decimal to float if needed
    if "price" in result.columns and result["price"].dtype == "object":
        result["price"] = result["price"].apply(
            lambda x: float(x) if isinstance(x, Decimal) else x
        )

    # Ensure required columns exist
    if "price" not in result.columns:
        result["price"] = 0.0
    if "quantity" not in result.columns:
        result["quantity"] = 0

    # Calculate metrics
    result["total"] = result["price"] * result["quantity"]
    result["tax"] = result["total"] * 0.1
    result["final_price"] = result["total"] + result["tax"]

    return result


def detect_outliers_external(
    df: pd.DataFrame, column_name: str = "price"
) -> pd.DataFrame:
    """Detect outliers using external processing.

    Args:
    ----
        df: Input DataFrame
        column_name: Column to check for outliers

    Returns:
    -------
        DataFrame with outlier detection columns

    """
    result = df.copy()

    if column_name in result.columns:
        # Calculate statistics
        mean_val = result[column_name].mean()
        std_val = result[column_name].std()

        if std_val > 0:
            result["z_score"] = (result[column_name] - mean_val) / std_val
            result["is_outlier"] = np.abs(result["z_score"]) > 3
            result["percentile"] = result[column_name].rank(pct=True) * 100
        else:
            result["z_score"] = 0
            result["is_outlier"] = False
            result["percentile"] = 50
    else:
        result["z_score"] = None
        result["is_outlier"] = None
        result["percentile"] = None

    return result


# ===== ADVANCED SCALAR UDF APPROACH =====
# Complex calculations using window functions and aggregates


@python_scalar_udf
def calculate_running_total(
    current_total: float, previous_running_total: float
) -> float:
    """Calculate running total for time series analysis."""
    if current_total is None:
        return previous_running_total or 0.0

    current_float = (
        float(current_total) if isinstance(current_total, Decimal) else current_total
    )
    previous_float = (
        float(previous_running_total)
        if isinstance(previous_running_total, Decimal)
        else (previous_running_total or 0.0)
    )

    return previous_float + current_float


@python_scalar_udf
def calculate_moving_average(values_sum: float, count: int) -> Optional[float]:
    """Calculate moving average."""
    if values_sum is None or count is None or count == 0:
        return None

    values_float = float(values_sum) if isinstance(values_sum, Decimal) else values_sum
    return values_float / count


@python_scalar_udf
def calculate_growth_rate(
    current_value: float, previous_value: float
) -> Optional[float]:
    """Calculate growth rate between two periods."""
    if current_value is None or previous_value is None or previous_value == 0:
        return None

    current_float = (
        float(current_value) if isinstance(current_value, Decimal) else current_value
    )
    previous_float = (
        float(previous_value) if isinstance(previous_value, Decimal) else previous_value
    )

    return ((current_float - previous_float) / previous_float) * 100


# ===== DATA QUALITY UDFs =====


@python_scalar_udf
def validate_email_format(email: str) -> bool:
    """Validate if email has proper format."""
    if not email or not isinstance(email, str):
        return False

    import re

    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))


@python_scalar_udf
def validate_price_range(
    price: float, min_price: float = 0.0, max_price: float = 10000.0
) -> bool:
    """Validate if price is within acceptable range."""
    if price is None:
        return False

    price_float = float(price) if isinstance(price, Decimal) else price
    return min_price <= price_float <= max_price


@python_scalar_udf
def calculate_data_quality_score(
    email_valid: bool, price_valid: bool, name_not_null: bool
) -> float:
    """Calculate overall data quality score."""
    score = 0.0
    total_checks = 3

    if email_valid:
        score += 1
    if price_valid:
        score += 1
    if name_not_null:
        score += 1

    return (score / total_checks) * 100
