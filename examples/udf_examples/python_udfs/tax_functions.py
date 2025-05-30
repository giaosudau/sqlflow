"""Tax functions for SQLFlow demo."""

import logging
from decimal import Decimal

from sqlflow.udfs import python_scalar_udf

# Set up logging with standard format
logger = logging.getLogger(__name__)


@python_scalar_udf
def apply_discount(price: float, discount_percent: float) -> float:
    """Apply a percentage discount to a price.

    Args:
    ----
        price: Original price
        discount_percent: Discount percentage (0-100)

    Returns:
    -------
        Discounted price

    """
    if price is None:
        return None

    # Handle Decimal type
    price_float = float(price) if isinstance(price, Decimal) else price
    discount_float = (
        float(discount_percent)
        if isinstance(discount_percent, Decimal)
        else discount_percent
    )

    return price_float * (1 - discount_float / 100)
