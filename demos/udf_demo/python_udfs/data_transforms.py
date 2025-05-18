"""Data transformation UDFs for SQLFlow demo."""

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
