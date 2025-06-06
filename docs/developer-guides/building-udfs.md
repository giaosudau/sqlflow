# Building SQLFlow Python UDFs

## Overview

SQLFlow's UDF (User-Defined Function) system allows seamless integration of Python functions into SQL queries. This guide covers the architecture, implementation patterns, and best practices for building production-ready UDFs.

**UDF Philosophy**
- **SQL-native integration**: Python functions work naturally within SQL queries
- **Automatic discovery**: UDFs are found and registered automatically from your codebase
- **Type safety**: Built-in validation and type checking for robust operation
- **Performance optimization**: Efficient execution with automatic optimization

## 🏗️ UDF Architecture

### UDF Types

SQLFlow supports two primary UDF types, implemented in `sqlflow/udfs/decorators.py`:

1. **Scalar UDFs**: Process individual values (row-by-row transformations)
2. **Table UDFs**: Process entire DataFrames (dataset-level transformations)

### Basic Implementation

```python
from sqlflow.udfs import python_scalar_udf, python_table_udf
import pandas as pd

# Scalar UDF - processes one value at a time
@python_scalar_udf
def calculate_tax(price: float, tax_rate: float = 0.1) -> float:
    """Calculate tax on a price with automatic type handling."""
    return price * (1 + tax_rate)

# Table UDF - processes entire datasets
@python_table_udf(
    output_schema={"customer_id": "INTEGER", "total": "DOUBLE", "tax": "DOUBLE"}
)
def add_tax_calculations(df: pd.DataFrame, tax_rate: float = 0.08) -> pd.DataFrame:
    """Add tax calculations to a DataFrame."""
    result = df.copy()
    result["tax"] = result["price"] * tax_rate
    result["total"] = result["price"] + result["tax"]
    return result
```

## 📋 Scalar UDF Implementation

### Simple Scalar UDF

```python
@python_scalar_udf
def format_phone(phone: str) -> str:
    """Format phone number to standard format."""
    if not phone:
        return None
    
    # Remove non-digits
    digits = ''.join(filter(str.isdigit, phone))
    
    # Format as (XXX) XXX-XXXX
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone

@python_scalar_udf
def calculate_discount(price: float, customer_tier: str) -> float:
    """Calculate discount based on customer tier."""
    tier_discounts = {
        'VIP': 0.20,
        'Premium': 0.15, 
        'Standard': 0.10
    }
    discount_rate = tier_discounts.get(customer_tier, 0.05)
    return price * discount_rate
```

### Advanced Scalar UDF with Custom Name

```python
@python_scalar_udf(name="calculate_shipping_cost")
def complex_shipping_calculation(weight: float, distance: float, 
                               priority: str = "standard") -> float:
    """Calculate shipping cost based on multiple factors."""
    base_rate = 2.50
    weight_factor = weight * 0.15
    distance_factor = distance * 0.008
    
    priority_multipliers = {
        "express": 2.0,
        "priority": 1.5,
        "standard": 1.0
    }
    
    multiplier = priority_multipliers.get(priority, 1.0)
    total_cost = (base_rate + weight_factor + distance_factor) * multiplier
    
    return round(total_cost, 2)
```

### Error Handling in Scalar UDFs

```python
@python_scalar_udf
def safe_divide(numerator: float, denominator: float) -> Optional[float]:
    """Safely divide two numbers with null handling."""
    if denominator == 0 or pd.isna(denominator) or pd.isna(numerator):
        return None
    return numerator / denominator

@python_scalar_udf
def validate_email(email: str) -> bool:
    """Validate email format with comprehensive checking."""
    if not email or pd.isna(email):
        return False
    
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email.strip()))
```

## 📊 Table UDF Implementation

### Basic Table UDF

```python
@python_table_udf(
    output_schema={
        "customer_id": "INTEGER",
        "order_id": "INTEGER", 
        "amount": "DOUBLE",
        "tax": "DOUBLE",
        "total": "DOUBLE"
    }
)
def calculate_order_totals(df: pd.DataFrame, tax_rate: float = 0.08) -> pd.DataFrame:
    """Calculate order totals with tax."""
    result = df.copy()
    result["tax"] = result["amount"] * tax_rate
    result["total"] = result["amount"] + result["tax"]
    return result
```

### Advanced Table UDF with Requirements

```python
@python_table_udf(
    required_columns=["customer_id", "order_date", "amount"],
    output_schema={
        "customer_id": "INTEGER",
        "order_date": "DATE",
        "amount": "DOUBLE", 
        "running_total": "DOUBLE",
        "avg_order_30_day": "DOUBLE"
    }
)
def calculate_customer_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate advanced customer metrics with windowing."""
    result = df.copy()
    
    # Sort by customer and date for window calculations
    result = result.sort_values(['customer_id', 'order_date'])
    
    # Calculate running total per customer
    result['running_total'] = result.groupby('customer_id')['amount'].cumsum()
    
    # Calculate 30-day rolling average
    result['avg_order_30_day'] = (
        result.groupby('customer_id')['amount']
        .rolling(window=30, min_periods=1)
        .mean()
        .reset_index(drop=True)
    )
    
    return result
```

### Schema Inference for Development

```python
@python_table_udf(infer=True)  # For development only
def experimental_transform(df: pd.DataFrame) -> pd.DataFrame:
    """Experimental UDF with schema inference."""
    # Schema will be inferred from first execution
    # Use this only for prototyping
    result = df.copy()
    result["new_column"] = result["existing_column"] * 2
    return result
```

## 🔍 UDF Discovery and Usage

### Automatic Discovery

SQLFlow automatically discovers UDFs from your `python_udfs/` directory:

```bash
# Project structure
my_project/
├── python_udfs/
│   ├── financial_functions.py    # Financial calculations
│   ├── text_processing.py        # Text manipulation
│   └── data_quality.py           # Data validation
├── pipelines/
│   └── my_pipeline.sf
└── profiles/
    └── default.yaml
```

### Usage in SQL

```sql
-- Use scalar UDFs in SELECT statements
CREATE TABLE processed_customers AS
SELECT 
    customer_id,
    name,
    PYTHON_FUNC("python_udfs.text_utils.format_phone", phone) as formatted_phone,
    PYTHON_FUNC("python_udfs.validation.validate_email", email) as email_is_valid,
    PYTHON_FUNC("python_udfs.financial.calculate_discount", lifetime_value, tier) as available_discount
FROM customers;

-- Use table UDFs with external processing pattern
CREATE TABLE enriched_orders AS
SELECT * FROM PYTHON_FUNC("python_udfs.data_transforms.calculate_order_totals", raw_orders);
```

### UDF Metadata and Introspection

```python
# UDFs include rich metadata for introspection
def inspect_udf(udf_function):
    """Inspect UDF metadata."""
    metadata = {
        "name": getattr(udf_function, "_udf_name", "unknown"),
        "type": getattr(udf_function, "_udf_type", "unknown"),
        "signature": getattr(udf_function, "_signature", "unknown"),
        "required_columns": getattr(udf_function, "_required_columns", []),
        "output_schema": getattr(udf_function, "_output_schema", {}),
        "docstring": udf_function.__doc__ or ""
    }
    return metadata
```

## 🧪 Testing UDFs

### Unit Testing Scalar UDFs

```python
import pytest
from python_udfs.financial_functions import calculate_tax, safe_divide

class TestScalarUDFs:
    def test_calculate_tax(self):
        """Test tax calculation UDF."""
        # Test normal operation
        assert calculate_tax(100.0, 0.1) == 110.0
        assert calculate_tax(50.0, 0.08) == 54.0
        
        # Test with None values
        assert calculate_tax(None, 0.1) is None
        
    def test_safe_divide(self):
        """Test safe division UDF."""
        assert safe_divide(10.0, 2.0) == 5.0
        assert safe_divide(10.0, 0.0) is None
        assert safe_divide(None, 2.0) is None
```

### Unit Testing Table UDFs

```python
import pandas as pd
from python_udfs.data_transforms import calculate_order_totals

class TestTableUDFs:
    def test_calculate_order_totals(self):
        """Test order totals calculation UDF."""
        # Create test DataFrame
        test_data = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "order_id": [101, 102, 103], 
            "amount": [100.0, 200.0, 150.0]
        })
        
        # Execute UDF
        result = calculate_order_totals(test_data, 0.08)
        
        # Verify results
        assert "tax" in result.columns
        assert "total" in result.columns
        assert result["tax"].iloc[0] == 8.0
        assert result["total"].iloc[0] == 108.0
```

### Integration Testing

```python
def test_udf_sql_integration():
    """Test UDF integration with SQL execution."""
    from sqlflow.core.engines.duckdb import DuckDBEngine
    
    engine = DuckDBEngine()
    
    # Register test data
    test_df = pd.DataFrame({
        "price": [100.0, 200.0],
        "tax_rate": [0.08, 0.10]
    })
    engine.connection.register("test_data", test_df)
    
    # Register UDF
    engine.register_python_udf("calculate_tax", calculate_tax)
    
    # Execute SQL with UDF
    result = engine.execute_query("""
        SELECT price, tax_rate, calculate_tax(price, tax_rate) as total
        FROM test_data
    """).fetchdf()
    
    assert len(result) == 2
    assert result["total"].iloc[0] == 108.0
```

## 🔧 Advanced UDF Features

### Default Parameter Handling

```python
from sqlflow.udfs.enhanced_udfs import duckdb_compatible_udf

@duckdb_compatible_udf
@python_scalar_udf
def flexible_calculation(base_value: float, multiplier: float = 1.5, 
                        bonus: float = 0.0) -> float:
    """UDF with multiple default parameters."""
    return base_value * multiplier + bonus

# This creates multiple UDF variants:
# - flexible_calculation(base_value, multiplier, bonus)
# - flexible_calculation_default_bonus(base_value, multiplier) 
# - flexible_calculation_defaults(base_value)
```

### Performance Optimization

```python
@python_table_udf(
    output_schema={"id": "INTEGER", "processed_value": "DOUBLE"}
)
def optimized_batch_processing(df: pd.DataFrame, 
                             batch_size: int = 10000) -> pd.DataFrame:
    """Process large datasets in optimized batches."""
    
    # Use vectorized operations for performance
    result = df.copy()
    
    # Avoid loops, use pandas vectorization
    result["processed_value"] = (
        result["value"].fillna(0) * 1.1 + 
        result["adjustment"].fillna(0)
    )
    
    # Use efficient pandas operations
    result["category"] = pd.Categorical(result["category"])
    
    return result
```

### Complex Business Logic

```python
@python_table_udf(
    required_columns=["transaction_date", "amount", "merchant_category"],
    output_schema={
        "transaction_date": "DATE",
        "amount": "DOUBLE", 
        "merchant_category": "VARCHAR",
        "risk_score": "DOUBLE",
        "fraud_flag": "BOOLEAN"
    }
)
def fraud_detection_scoring(df: pd.DataFrame) -> pd.DataFrame:
    """Advanced fraud detection with multiple factors."""
    result = df.copy()
    
    # Initialize risk score
    result["risk_score"] = 0.0
    
    # Amount-based risk
    result.loc[result["amount"] > 1000, "risk_score"] += 0.3
    result.loc[result["amount"] > 5000, "risk_score"] += 0.5
    
    # Category-based risk
    high_risk_categories = ["gambling", "cash_advance", "crypto"]
    result.loc[result["merchant_category"].isin(high_risk_categories), "risk_score"] += 0.4
    
    # Time-based patterns (simplified)
    result["hour"] = pd.to_datetime(result["transaction_date"]).dt.hour
    result.loc[(result["hour"] < 6) | (result["hour"] > 23), "risk_score"] += 0.2
    
    # Final fraud flag
    result["fraud_flag"] = result["risk_score"] > 0.7
    
    # Clean up temporary columns
    result = result.drop(columns=["hour"])
    
    return result
```

## 🚀 Best Practices

### Design Principles

1. **Handle null values gracefully**: Always check for None/NaN inputs
2. **Use type hints**: Improves integration and error detection
3. **Document thoroughly**: Include docstrings and parameter descriptions
4. **Optimize for performance**: Use vectorized operations in table UDFs
5. **Test comprehensively**: Unit tests for all code paths

### Error Handling

```python
@python_scalar_udf
def robust_calculation(value: float, factor: float) -> Optional[float]:
    """Example of robust UDF error handling."""
    try:
        # Handle null/NaN values
        if pd.isna(value) or pd.isna(factor):
            return None
            
        # Handle edge cases
        if factor == 0:
            return 0.0
            
        # Perform calculation
        result = value * factor
        
        # Validate result
        if pd.isna(result) or not pd.isfinite(result):
            return None
            
        return result
        
    except Exception:
        # Log error if needed, return null for safety
        return None
```

### Documentation Standards

```python
@python_table_udf(
    required_columns=["customer_id", "order_date", "amount"],
    output_schema={
        "customer_id": "INTEGER",
        "order_date": "DATE", 
        "amount": "DOUBLE",
        "ltv_score": "DOUBLE"
    }
)
def calculate_customer_ltv(df: pd.DataFrame, 
                          lookback_days: int = 365) -> pd.DataFrame:
    """Calculate Customer Lifetime Value score.
    
    This UDF calculates a simplified LTV score based on historical
    transaction patterns within a specified lookback period.
    
    Args:
        df: DataFrame with customer transaction data
        lookback_days: Number of days to look back for LTV calculation
        
    Returns:
        DataFrame with additional ltv_score column
        
    Business Logic:
        - LTV = (Average Order Value) × (Purchase Frequency) × (Lookback Period)
        - Purchase Frequency = Total Orders / Days Active
        - Handles customers with single transactions gracefully
        
    Performance Notes:
        - Optimized for datasets up to 1M rows
        - Uses vectorized pandas operations
        - Memory usage scales linearly with input size
    """
    # Implementation here...
```

## 🔍 Debugging and Monitoring

### UDF Performance Monitoring

```python
import time
from functools import wraps

def monitor_performance(func):
    """Decorator to monitor UDF performance."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        
        # Log performance metrics
        print(f"UDF {func.__name__} executed in {execution_time:.3f}s")
        
        return result
    return wrapper

@monitor_performance
@python_scalar_udf
def monitored_calculation(x: float) -> float:
    """UDF with performance monitoring."""
    return x * 2.5
```

### Debugging UDF Issues

```sql
-- Test UDF registration
SELECT name, type FROM sqlflow_registered_udfs WHERE name LIKE '%my_udf%';

-- Test UDF execution with sample data
SELECT my_udf(test_column) FROM (VALUES (1), (2), (3)) AS t(test_column);

-- Examine UDF metadata
DESCRIBE FUNCTION my_udf;
```

## 📚 Learning Resources

### Example Implementations

Study existing UDF patterns:

- **UDF Decorators**: `sqlflow/udfs/decorators.py`
- **UDF Manager**: `sqlflow/udfs/manager.py` 
- **Example UDFs**: `examples/udf_examples/python_udfs/`
- **Test Cases**: `tests/unit/udfs/`

### Documentation References

- **[UDF System](udf-system.md)**: Complete UDF architecture and implementation
- **[Architecture Overview](architecture-overview.md)**: System design principles
- **[State Management](state-management.md)**: Integration with incremental processing

---

**Ready to build UDFs?** Start with simple scalar functions and gradually move to more complex table UDFs. The automatic discovery and registration system handles the infrastructure, so you can focus on your business logic. 