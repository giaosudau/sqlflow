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

We implemented two approaches to address this issue:

### 1. Function Specialization through Enhanced UDF Manager

We created a specialized version of the UDF manager (`enhanced_manager.py`) that automatically creates additional specialized versions of UDFs with default parameters. For each parameter with a default value, the enhanced manager creates a new UDF that doesn't require that parameter.

For example, for the `calculate_tax` function with a default `tax_rate` parameter, the enhanced manager creates a specialized version named `calculate_tax_without_tax_rate` that can be called with just the price parameter.

This approach provides a clean, declarative way to handle UDFs with default parameters without modifying existing code.

### 2. Creating Custom Specialized UDFs

For specific cases, developers can also manually create specialized versions of functions with fixed default parameters:

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

### Using the Enhanced UDF Manager

The enhanced UDF manager is automatically integrated with SQLFlow's executor system. When discovering UDFs, it automatically creates and registers specialized versions for UDFs with default parameters.

To use a UDF with a default parameter omitted:

```sql
-- Original UDF has signature: calculate_tax(price, tax_rate=0.1)
-- Use the specialized version without the tax_rate parameter
SELECT PYTHON_FUNC("python_udfs.enhanced_udfs.calculate_tax.without_tax_rate", price) FROM sales;
```

### Using Custom Specialized UDFs

If you've created custom specialized UDFs, you can call them directly:

```sql
-- Use the specialized version that applies the default tax rate
SELECT PYTHON_FUNC("python_udfs.tax_functions.calculate_tax_default", price) FROM sales;
```

## Implementation Details

The key components of our implementation include:

1. **Enhanced UDF Manager** (`sqlflow/udfs/enhanced_manager.py`): Enriches the UDF discovery process to create specialized versions of UDFs with default parameters.

2. **Executor Integration**: We modified `BaseExecutor` to use the enhanced UDF manager for automatic specialization of UDFs.

3. **Decorator Utility** (`sqlflow/demos/udf_demo/python_udfs/enhanced_udfs.py`): Provides a `duckdb_compatible_udf` decorator for developers who want to explicitly handle default parameters in a more controlled way.

## Best Practices

1. When designing UDFs with default parameters, be aware that you'll need to use the specialized versions when calling them with fewer arguments.

2. Use meaningful parameter names so that the generated specialized UDF names make sense (e.g., `calculate_tax_without_tax_rate`).

3. Consider documenting the available specialized UDFs in your project for easier reference by SQL authors.

4. For complex cases with multiple default parameters, you may want to create explicit specialized UDFs to avoid auto-generated names that could be confusing.
