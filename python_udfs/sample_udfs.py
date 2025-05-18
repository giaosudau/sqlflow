"""Sample UDFs for SQLFlow."""

import pandas as pd
from sqlflow.udfs import python_scalar_udf, python_table_udf

@python_scalar_udf
def capitalize_words(text: str) -> str:
    """Capitalize each word in a string.
    
    Args:
        text: Input string
        
    Returns:
        String with each word capitalized
    """
    if text is None:
        return None
    return ' '.join(word.capitalize() for word in text.split())

@python_scalar_udf(name="add_tax")
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

@python_table_udf
def add_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add sales metrics to a DataFrame.
    
    Args:
        df: Input DataFrame with 'price' and 'quantity' columns
        
    Returns:
        DataFrame with 'total' column added
    """
    result = df.copy()
    if 'price' in result.columns and 'quantity' in result.columns:
        result['total'] = result['price'] * result['quantity']
    return result 