"""Example User-Defined Functions (UDFs) for SQLFlow.

This module demonstrates how to define and use Python UDFs with SQLFlow.
"""

import pandas as pd

from sqlflow.udfs.decorators import python_scalar_udf, python_table_udf


@python_scalar_udf
def calculate_discount(price: float, rate: float = 0.1) -> float:
    """Calculate discount amount.

    Args:
        price: Original price
        rate: Discount rate (default: 0.1)

    Returns:
        Discount amount
    """
    if price is None:
        return None
    return price * rate


@python_scalar_udf(name="final_price")
def calculate_final_price(price: float, discount_rate: float = 0.1) -> float:
    """Calculate final price after discount.

    Args:
        price: Original price
        discount_rate: Discount rate (default: 0.1)

    Returns:
        Final price after discount
    """
    if price is None:
        return None
    return price * (1 - discount_rate)


@python_table_udf
def add_price_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add price-related metrics to a DataFrame.

    Args:
        df: DataFrame with 'price' column

    Returns:
        DataFrame with additional price metrics
    """
    result = df.copy()
    result["discount_10"] = result["price"] * 0.1
    result["discount_20"] = result["price"] * 0.2
    result["final_price_10"] = result["price"] - result["discount_10"]
    result["final_price_20"] = result["price"] - result["discount_20"]
    return result


@python_table_udf(name="sales_summary")
def calculate_sales_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate sales summary by category.

    Args:
        df: DataFrame with 'category', 'price', and 'quantity' columns

    Returns:
        Summary DataFrame with sales metrics by category
    """
    # Calculate total sale amount
    df = df.copy()
    df["total"] = df["price"] * df["quantity"]

    # Group by category
    summary = (
        df.groupby("category")
        .agg({"price": ["mean", "min", "max"], "quantity": "sum", "total": "sum"})
        .reset_index()
    )

    # Flatten the column names
    summary.columns = ["_".join(col).strip("_") for col in summary.columns.values]

    return summary
