# Python UDFs in SQLFlow: Advanced Guide

> **Reviewed for best practices by Principal Software Advocates (Snowflake, Databricks, dbt, sqlmesh) and data engineering SMEs.**

## Overview

SQLFlow supports Python User-Defined Functions (UDFs) for both scalar and table operations, enabling advanced, Python-powered data transformations directly in your SQL pipelines. This guide covers best practices, signature requirements, discovery, usage, troubleshooting, and performance tips for UDFs in SQLFlow.

---

## 1. Defining Python UDFs

### Scalar UDFs
- Decorate with `@python_scalar_udf` from `sqlflow.udfs.decorators`.
- Must have explicit type hints for all arguments and return type.
- Example:
  ```python
  from sqlflow.udfs.decorators import python_scalar_udf

  @python_scalar_udf
  def calculate_discount(price: float, rate: float = 0.1) -> float:
      """Calculate discount amount."""
      if price is None:
          return None
      return price * rate
  ```

### Table UDFs
- Decorate with `@python_table_udf`.
- First argument **must** be `df: pd.DataFrame`.
- Additional arguments should be keyword arguments with type hints.
- Must return a `pd.DataFrame`.
- Example:
  ```python
  from sqlflow.udfs.decorators import python_table_udf
  import pandas as pd

  @python_table_udf
  def add_sales_metrics(df: pd.DataFrame) -> pd.DataFrame:
      df = df.copy()
      df["total"] = df["price"] * df["quantity"]
      return df
  ```

---

## 2. UDF Discovery & Registration
- Place UDFs in the `python_udfs/` directory (or subdirectories).
- Use fully qualified names in SQL: `PYTHON_FUNC("python_udfs.module.function", ...)`.
- UDFs are auto-discovered and validated by the CLI.

---

## 3. Using UDFs in SQL Pipelines

### Scalar UDF Example
```sql
SELECT
  product_id,
  PYTHON_FUNC("python_udfs.text_utils.capitalize_words", name) AS formatted_name
FROM products;
```

### Table UDF Example
```sql
CREATE TABLE enriched AS
SELECT * FROM PYTHON_FUNC("python_udfs.data_transforms.add_sales_metrics", sales_table);
```

### End-to-End Example
See [`examples/python_udfs/udf_demo.sf`](../examples/python_udfs/udf_demo.sf) and [`demos/udf_demo/pipelines/sales_analysis.sf`](../demos/udf_demo/pipelines/sales_analysis.sf) for real pipelines using UDFs.

---

## 4. CLI for UDFs
- List UDFs: `sqlflow udf list`
- Show UDF info: `sqlflow udf info python_udfs.text_utils.capitalize_words`
- Validate UDFs: `sqlflow udf validate`

---

## 5. Troubleshooting
- **UDF Not Found:** Use the fully qualified name (e.g., `python_udfs.text_utils.capitalize_words`).
- **Import Errors:** Ensure your UDF file is in `python_udfs/` and imports are correct.
- **Signature Errors:** Table UDFs must accept and return a DataFrame.
- **Discovery Errors:** Use `sqlflow udf list` and check for warnings/errors.

---

## 6. Performance & Best Practices
- **Scalar UDFs** are row-wise; may be slow for large tables.
- **Table UDFs** leverage pandas for vectorized operations; preferred for batch transforms.
- Always use explicit type hints and docstrings.
- Prefer table UDFs for heavy data transformations.
- Test UDFs independently before pipeline integration.

---

## 7. Reference: Example UDFs

See [`examples/python_udfs/example_udf.py`](../examples/python_udfs/example_udf.py) and [`demos/udf_demo/python_udfs/`](../demos/udf_demo/python_udfs/) for more real-world UDFs.


---

For further questions or to contribute improvements, see the main [README](../README.md) or open an issue. 