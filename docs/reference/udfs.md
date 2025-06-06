# SQLFlow UDF Reference

Complete reference for Python User-Defined Functions (UDFs) in SQLFlow with verified patterns and type specifications.

## Overview

SQLFlow UDFs allow seamless integration of Python functions into SQL queries. The UDF system provides automatic discovery, type safety, and performance optimization while maintaining SQL-native syntax.

**Quick Reference:**
```python
# Scalar UDF - process individual values
@python_scalar_udf
def calculate_tax(price: float, tax_rate: float = 0.1) -> float:
    return price * (1 + tax_rate)

# Table UDF - process entire DataFrames  
@python_table_udf(output_schema={"id": "INTEGER", "total": "DOUBLE"})
def process_orders(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["total"] = result["price"] * result["quantity"]
    return result
```

**Usage in SQL:**
```sql
-- Use scalar UDFs directly in SELECT statements
SELECT 
    customer_id, 
    PYTHON_FUNC("python_udfs.tax_functions.calculate_tax", amount, 0.08) as tax_amount 
FROM orders;

-- Use table UDFs in FROM clauses  
SELECT * FROM PYTHON_FUNC("python_udfs.data_transforms.add_sales_metrics", raw_sales);
```

## UDF Types

### Scalar UDFs

Process individual values row-by-row. Best for calculations, formatting, and data validation.

**Basic Pattern:**
```python
from sqlflow.udfs import python_scalar_udf

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
```

**With Default Parameters:**
```python
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

**Custom UDF Name:**
```python
@python_scalar_udf(name="shipping_cost")
def calculate_shipping_cost(weight: float, distance: float, 
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

### Table UDFs

Process entire DataFrames for complex transformations, aggregations, and multi-column operations.

**Basic Pattern:**
```python
from sqlflow.udfs import python_table_udf
import pandas as pd

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

**With Required Columns:**
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
    
    # Sort by customer and date
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

## UDF Decorators

### @python_scalar_udf

**Signature:**
```python
@python_scalar_udf(func=None, *, name: Optional[str] = None)
```

**Parameters:**
- `func`: Python function to register as UDF
- `name`: Optional custom name for the UDF (defaults to function name)

**Metadata Added:**
- `_is_sqlflow_udf = True`
- `_udf_type = "scalar"`
- `_udf_name = name or func.__name__`

### @python_table_udf

**Signature:**
```python
@python_table_udf(
    func=None, 
    *,
    name: Optional[str] = None,
    required_columns: Optional[List[str]] = None,
    output_schema: Optional[Dict[str, str]] = None,
    infer: bool = False
)
```

**Parameters:**
- `func`: Python function to register as UDF
- `name`: Optional custom name for the UDF
- `required_columns`: List of required input column names
- `output_schema`: Expected output schema with column types
- `infer`: Whether to infer schema from first execution (development only)

**Metadata Added:**
- `_is_sqlflow_udf = True`
- `_udf_type = "table"`
- `_udf_name = name or func.__name__`
- `_required_columns = required_columns`
- `_output_schema = output_schema`

## Type System

### Supported Input Types

**Scalar UDFs:**
```python
def example_types(
    integer_val: int,
    float_val: float,
    string_val: str,
    boolean_val: bool,
    optional_val: Optional[str] = None,
    date_val: datetime.date,
    datetime_val: datetime.datetime
) -> float:
    pass
```

**Table UDFs:**
```python
def table_example(
    df: pd.DataFrame,           # Required first parameter
    param1: int = 100,          # Additional parameters must be keyword-only
    param2: str = "default"
) -> pd.DataFrame:
    pass
```

### SQL Type Mapping

| Python Type | SQL Type | Notes |
|-------------|----------|-------|
| `int` | `INTEGER` | 64-bit signed integer |
| `float` | `DOUBLE` | Double precision floating point |
| `str` | `VARCHAR` | Variable-length string |
| `bool` | `BOOLEAN` | True/False values |
| `datetime.date` | `DATE` | Date only |
| `datetime.datetime` | `TIMESTAMP` | Date and time |
| `Optional[T]` | `T` | Nullable version of type T |
| `pd.DataFrame` | `TABLE` | Table UDF input/output |

### Output Schema Specification

```python
# Comprehensive schema example
@python_table_udf(
    output_schema={
        "id": "INTEGER",
        "name": "VARCHAR(255)",
        "amount": "DECIMAL(10,2)",
        "created_at": "TIMESTAMP",
        "is_active": "BOOLEAN",
        "metadata": "JSON"
    }
)
def process_data(df: pd.DataFrame) -> pd.DataFrame:
    pass
```

**Supported SQL Types:**
- `INTEGER`, `BIGINT`, `SMALLINT`
- `DECIMAL(p,s)`, `NUMERIC(p,s)`
- `DOUBLE`, `FLOAT`, `REAL`
- `VARCHAR(n)`, `CHAR(n)`, `TEXT`
- `BOOLEAN`
- `DATE`, `TIME`, `TIMESTAMP`
- `JSON`

## UDF Discovery

SQLFlow automatically discovers UDFs from your project structure:

```
project/
├── python_udfs/
│   ├── __init__.py
│   ├── financial_functions.py    # Financial calculations
│   ├── text_processing.py        # Text manipulation
│   └── data_quality.py           # Data validation
├── pipelines/
└── profiles/
```

**Discovery Process:**
1. Scan `python_udfs/` directory for `.py` files
2. Import Python modules safely
3. Inspect functions for UDF decorators
4. Extract metadata (name, type, signature)
5. Register with SQL execution engine

**Using UDFs in SQL:**

UDFs are automatically discovered from your `python_udfs/` directory and can be used directly:

```sql
-- Scalar UDFs in SELECT statements  
SELECT 
    customer_id,
    PYTHON_FUNC("python_udfs.financial_functions.calculate_ltv", revenue, churn_rate) as ltv
FROM customers;

-- Table UDFs in FROM clauses (see limitations section below)
SELECT * FROM PYTHON_FUNC("python_udfs.data_transforms.add_sales_metrics", raw_sales);
```

## Error Handling

### Null Value Handling

**Best Practice Pattern:**
```python
@python_scalar_udf
def safe_divide(numerator: float, denominator: float) -> Optional[float]:
    """Safely divide two numbers with null handling."""
    if denominator == 0 or pd.isna(denominator) or pd.isna(numerator):
        return None
    return numerator / denominator
```

### Exception Handling

**Robust Error Handling:**
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
        # Return null for safety
        return None
```

### Table UDF Validation

**Input Validation:**
```python
@python_table_udf(
    required_columns=["id", "amount"],
    output_schema={"id": "INTEGER", "processed_amount": "DOUBLE"}
)
def validate_and_process(df: pd.DataFrame) -> pd.DataFrame:
    """Example with comprehensive validation."""
    
    # Validation happens automatically for required_columns
    # Additional custom validation
    if df.empty:
        return pd.DataFrame(columns=["id", "processed_amount"])
    
    if "amount" in df.columns and df["amount"].isna().all():
        # Handle all-null amounts
        df = df.fillna({"amount": 0})
    
    result = df.copy()
    result["processed_amount"] = result["amount"] * 1.1
    
    return result
```

## Performance Patterns

### Vectorized Operations

**Efficient Scalar UDF:**
```python
@python_scalar_udf
def efficient_calculation(value: float) -> float:
    """Use NumPy for performance when possible."""
    import numpy as np
    
    # Vectorized operations are faster
    return float(np.sqrt(value * 2.5))
```

**Efficient Table UDF:**
```python
@python_table_udf(
    output_schema={"id": "INTEGER", "normalized": "DOUBLE"}
)
def vectorized_normalization(df: pd.DataFrame) -> pd.DataFrame:
    """Use pandas vectorization for performance."""
    result = df.copy()
    
    # Vectorized operations - much faster than loops
    mean_val = result["value"].mean()
    std_val = result["value"].std()
    result["normalized"] = (result["value"] - mean_val) / std_val
    
    return result
```

### Memory Management

**Large Dataset Processing:**
```python
@python_table_udf(
    output_schema={"id": "INTEGER", "processed": "DOUBLE"}
)
def memory_efficient_processing(df: pd.DataFrame) -> pd.DataFrame:
    """Handle large datasets efficiently."""
    
    # Process in chunks for memory efficiency
    if len(df) > 100000:
        chunk_size = 10000
        chunks = []
        
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start:start + chunk_size].copy()
            # Process chunk
            chunk["processed"] = chunk["value"] * 2
            chunks.append(chunk)
        
        return pd.concat(chunks, ignore_index=True)
    else:
        # Small dataset - process normally
        result = df.copy()
        result["processed"] = result["value"] * 2
        return result
```

## Table UDF Limitations & Workarounds

### DuckDB Table Function Limitation

**Current Reality:** DuckDB's Python API has limitations with table functions in SQL FROM clauses.

```sql
-- ❌ This syntax has limitations in DuckDB Python API:
SELECT * FROM PYTHON_FUNC("process_orders", raw_orders);

-- ✅ This works for scalar UDFs:
SELECT *, PYTHON_FUNC("process_value", column_name) as processed FROM raw_orders;
```

### Workaround 1: Scalar UDF Chain

Break table operations into sequential scalar UDF steps:

```python
# File: python_udfs/order_processing.py
@python_scalar_udf
def calculate_sales_total(price: float, quantity: int) -> Optional[float]:
    """Calculate total for a sale (price * quantity)."""
    if price is None or quantity is None:
        return None
    return float(price) * quantity

@python_scalar_udf
def calculate_sales_tax(total: float, tax_rate: float = 0.1) -> Optional[float]:
    """Calculate tax on a total amount."""
    if total is None:
        return None
    return float(total) * tax_rate
```

```sql
-- Use scalar UDFs in sequence
CREATE TABLE sales_with_totals AS
SELECT 
    *,
    calculate_sales_total(price, quantity) AS total
FROM raw_sales;

CREATE TABLE sales_with_tax AS
SELECT 
    *,
    calculate_sales_tax(total) AS tax
FROM sales_with_totals;
```

### Workaround 2: External Processing

Fetch data → Process with pandas → Register back to DuckDB:

```python
# File: process_table_udf.py
from sqlflow.core.engines.duckdb import DuckDBEngine
from python_udfs.data_transforms import calculate_order_totals

# Initialize engine
engine = DuckDBEngine()

# Fetch data from DuckDB
result = engine.execute_query("SELECT * FROM raw_orders")
df = result.fetchdf()

# Process with table UDF
processed_df = calculate_order_totals(df, tax_rate=0.08)

# Register back to DuckDB
engine.connection.register("processed_orders", processed_df)

# Continue with SQL
summary = engine.execute_query("""
    SELECT product, AVG(total) as avg_total
    FROM processed_orders
    GROUP BY product
""")
```

## Testing UDFs

### Unit Testing Scalar UDFs

```python
import pytest
from python_udfs.financial_functions import calculate_tax, safe_divide

class TestScalarUDFs:
    def test_calculate_tax(self):
        """Test tax calculation UDF."""
        assert calculate_tax(100.0, 0.1) == 110.0
        assert calculate_tax(50.0, 0.08) == 54.0
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
        test_data = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "order_id": [101, 102, 103],
            "amount": [100.0, 200.0, 150.0]
        })
        
        result = calculate_order_totals(test_data, 0.08)
        
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
    
    # Execute SQL with UDF
    result = engine.execute_query("""
        SELECT price, tax_rate, calculate_tax(price, tax_rate) as total
        FROM test_data
    """).fetchdf()
    
    assert len(result) == 2
    assert result["total"].iloc[0] == 108.0
```

## CLI Commands

### UDF Management

```bash
# List all available UDFs
sqlflow udf list

# Create new UDF from template
sqlflow udf create my_function --type scalar
sqlflow udf create process_data --type table

# Test UDF functionality
sqlflow udf test calculate_tax

# Validate UDF syntax
sqlflow udf validate
```

### Development Workflow

```bash
# 1. Create UDF
sqlflow udf create profit_margin --type scalar

# 2. Edit the generated function
# vim python_udfs/profit_margin.py

# 3. Test UDF
sqlflow udf test profit_margin

# 4. Use in pipeline
sqlflow pipeline run analysis --verbose
```

## Best Practices

### Design Principles

1. **Handle null values gracefully** - Always check for None/NaN inputs
2. **Use type hints** - Improves integration and error detection
3. **Document thoroughly** - Include docstrings and parameter descriptions
4. **Optimize for performance** - Use vectorized operations
5. **Test comprehensively** - Unit tests for all code paths

### Naming Conventions

```python
# Good naming patterns
@python_scalar_udf
def calculate_customer_ltv(revenue: float, churn_rate: float) -> float:
    """Calculate customer lifetime value."""
    pass

@python_table_udf(name="enrich_order_data")
def enrich_orders_with_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich order data with customer information."""
    pass
```

### Documentation Standards

```python
@python_table_udf(
    required_columns=["customer_id", "order_date", "amount"],
    output_schema={
        "customer_id": "INTEGER",
        "ltv_score": "DOUBLE"
    }
)
def calculate_customer_ltv(df: pd.DataFrame, 
                          lookback_days: int = 365) -> pd.DataFrame:
    """Calculate Customer Lifetime Value score.
    
    Args:
        df: DataFrame with customer transaction data
        lookback_days: Number of days to look back for calculation
        
    Returns:
        DataFrame with additional ltv_score column
        
    Business Logic:
        LTV = (Average Order Value) × (Purchase Frequency) × (Lookback Period)
        
    Performance Notes:
        - Optimized for datasets up to 1M rows
        - Memory usage scales linearly
    """
    # Implementation here...
```

## Debugging

### UDF Inspection

```python
# Get UDF metadata
def inspect_udf(udf_function):
    """Inspect UDF metadata."""
    return {
        "name": getattr(udf_function, "_udf_name", "unknown"),
        "type": getattr(udf_function, "_udf_type", "unknown"),
        "required_columns": getattr(udf_function, "_required_columns", []),
        "output_schema": getattr(udf_function, "_output_schema", {}),
        "docstring": udf_function.__doc__ or ""
    }
```

### SQL Testing

```sql
-- Test UDF registration
SELECT name, type FROM sqlflow_registered_udfs WHERE name LIKE '%my_udf%';

-- Test UDF execution
SELECT my_udf(test_column) FROM (VALUES (1), (2), (3)) AS t(test_column);

-- Examine UDF metadata
DESCRIBE FUNCTION my_udf;
```

---

**Related Documentation:**
- [Building UDFs Guide](../developer-guides/building-udfs.md) - Step-by-step UDF development
- [UDF System Architecture](../developer-guides/udf-system.md) - Complete technical implementation
- [Analytics Pipeline Guide](../user-guides/building-analytics-pipelines.md) - Using UDFs in pipelines 