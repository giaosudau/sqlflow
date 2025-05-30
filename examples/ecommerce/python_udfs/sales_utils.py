"""Sales utility UDFs for SQLFlow ecommerce example."""

import logging

from sqlflow.udfs import python_scalar_udf

# Set up logging
logger = logging.getLogger(__name__)


@python_scalar_udf
def calculate_discount(price: float, region: str) -> float:
    """Calculate region-specific discount.

    Args:
    ----
        price: Original price
        region: Geographic region

    Returns:
    -------
        Discounted price

    """
    if price is None:
        return None

    discount_rates = {
        "us-east": 0.10,  # 10% discount
        "us-west": 0.15,  # 15% discount
        "eu": 0.08,  # 8% discount
        "asia": 0.12,  # 12% discount
        "latam": 0.20,  # 20% discount
        "middle-east": 0.10,  # 10% discount
    }

    # Apply default discount if region not found
    discount_rate = discount_rates.get(region, 0.05)
    return round(price * (1 - discount_rate), 2)


@python_scalar_udf
def categorize_customer(account_type: str, order_count: int) -> str:
    """Categorize customer based on account type and order count.

    Args:
    ----
        account_type: Customer account type
        order_count: Number of orders placed

    Returns:
    -------
        Customer category

    """
    if account_type is None:
        return "Unknown"

    account_type = account_type.lower()

    if account_type == "premium":
        if order_count >= 10:
            return "VIP"
        elif order_count >= 5:
            return "Loyal Premium"
        else:
            return "Premium"
    else:  # standard account
        if order_count >= 15:
            return "Frequent"
        elif order_count >= 7:
            return "Regular"
        else:
            return "Standard"


@python_scalar_udf
def format_currency(amount: float, region: str) -> str:
    """Format amount as currency based on region.

    Args:
    ----
        amount: Monetary amount
        region: Geographic region

    Returns:
    -------
        Formatted currency string

    """
    if amount is None:
        return None

    currency_formats = {
        "us-east": ("$", ""),
        "us-west": ("$", ""),
        "eu": ("€", ""),
        "asia": ("¥", ""),
        "latam": ("$", " MXN"),
        "middle-east": ("", " AED"),
    }

    # Default to US format if region not found
    prefix, suffix = currency_formats.get(region, ("$", ""))
    formatted = f"{prefix}{amount:.2f}{suffix}"
    return formatted


@python_scalar_udf
def calculate_shipping_cost(quantity: int, region: str) -> float:
    """Calculate shipping cost based on quantity and region.

    Args:
    ----
        quantity: Number of items
        region: Geographic region

    Returns:
    -------
        Shipping cost

    """
    base_rates = {
        "us-east": 5.99,
        "us-west": 6.99,
        "eu": 8.99,
        "asia": 12.99,
        "latam": 9.99,
        "middle-east": 14.99,
    }

    # Default to highest rate if region not found
    base_rate = base_rates.get(region, 15.99)

    # Add $2 for each additional item beyond first
    additional_items = max(0, quantity - 1)
    return round(base_rate + (additional_items * 2.0), 2)
