# UDF Default Parameters in SQLFlow

This documentation explains how default parameters in User-Defined Functions (UDFs) are handled in SQLFlow, and details our approach to fixing the issue with scalar UDFs that have default parameters.

## The Issue

When working with scalar UDFs that have default parameters, DuckDB does not natively support calling these functions with fewer arguments than the function signature specifies. For example, if a Python UDF is defined as:

```python
@python_scalar_udf
def calculate_tax(price: float, tax_rate: float = 0.1) -> float:
    return price * (1 + tax_rate)
```

DuckDB will register this as a function requiring two parameters, even though the second parameter has a default value. This means that when calling the function in SQL with only one argument:

```sql
SELECT PYTHON_FUNC("python_udfs.data_transforms.calculate_tax", price) FROM sales;
```

DuckDB will raise an error because it expects two arguments:

```
No function matches the given name and argument types 'calculate_tax(DOUBLE)'. 
You might need to add explicit type casts.
Candidate functions:
calculate_tax(DOUBLE, DOUBLE) -> DOUBLE
```

## The Solution

Our approach to address this issue is to create custom specialized UDFs for common default parameter patterns:

### Creating Custom Specialized UDFs

For functions with default parameters, we create specialized versions with fixed default parameters:

```python
@python_scalar_udf
def calculate_tax(price: float, tax_rate: float = 0.1) -> float:
    """Calculate the price with tax added."""
    if price is None:
        return None
    return price * (1 + tax_rate)

@python_scalar_udf
def calculate_tax_default(price: float) -> float:
    """Calculate the price with default tax rate (10%) added."""
    return calculate_tax(price, 0.1)
```

## Usage

### Using Custom Specialized UDFs

When you need to use a function with a default parameter, call the specialized version directly:

```sql
-- Use the specialized version that applies the default tax rate
SELECT PYTHON_FUNC("python_udfs.tax_functions.calculate_tax_default", price) FROM sales;
```

## Implementation Details

The key components of our implementation include:

1. **Enhanced UDF Manager** (`sqlflow/udfs/enhanced_manager.py`): Enriches the UDF discovery process to handle UDFs with default parameters.

2. **Executor Integration**: We modified `BaseExecutor` to properly integrate UDFs with the execution engine.

## Best Practices

1. When designing UDFs with default parameters, create specialized versions for common parameter combinations.

2. Use meaningful function names for your specialized UDFs (e.g., `calculate_tax_default` instead of `calculate_tax_without_tax_rate`).

3. Consider documenting your UDFs to make them easier to discover and use.

4. For complex cases with multiple default parameters, create explicit specialized UDFs with clear names and documentation.
