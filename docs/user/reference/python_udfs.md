<!-- filepath: /Users/chanhle/ai-playground/sqlflow/docs/user/reference/python_udfs.md -->
# Python UDFs in SQLFlow: Advanced Guide

## Overview

SQLFlow supports Python User-Defined Functions (UDFs) for both scalar and table operations, enabling advanced, Python-powered data transformations directly in your SQL pipelines. This guide covers best practices, signature requirements, discovery, usage, troubleshooting, and performance tips for UDFs in SQLFlow.

---

## 1. Defining Python UDFs

### Scalar UDFs
- Decorate with `@python_scalar_udf` from `sqlflow.udfs`.
- Must have explicit type hints for all arguments and return type.
- Example:
  ```python
  from sqlflow.udfs import python_scalar_udf

  @python_scalar_udf
  def capitalize_words(text: str) -> str:
      """Capitalize each word in a string."""
      if text is None:
          return None
      return " ".join(word.capitalize() for word in text.split())
  ```

### Table UDFs
- Decorate with `@python_table_udf` from `sqlflow.udfs`.
- First argument **must** be `df: pd.DataFrame`.
- Additional arguments should be keyword arguments with type hints.
- Must return a `pd.DataFrame`.
- Can specify output schema with the `output_schema` parameter.
- Example:
  ```python
  from sqlflow.udfs import python_table_udf
  import pandas as pd

  @python_table_udf(
      output_schema={
          "price": "DOUBLE",
          "quantity": "INTEGER",
          "total": "DOUBLE",
          "tax": "DOUBLE",
          "final_price": "DOUBLE",
      }
  )
  def add_sales_metrics(df: pd.DataFrame) -> pd.DataFrame:
      result = df.copy()
      
      # Calculate total
      result["total"] = result["price"] * result["quantity"]
      
      # Calculate tax at 10%
      result["tax"] = result["total"] * 0.1
      
      # Calculate final price
      result["final_price"] = result["total"] + result["tax"]
      
      return result
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
  id,
  PYTHON_FUNC("python_udfs.text_utils.capitalize_words", name) AS formatted_name,
  PYTHON_FUNC("python_udfs.text_utils.extract_domain", email) AS email_domain,
  PYTHON_FUNC("python_udfs.text_utils.count_words", notes) AS note_word_count
FROM customers;
```

### Table UDF Example
```sql
-- Process a table with a table UDF
CREATE TABLE sales_with_metrics AS
SELECT * 
FROM PYTHON_FUNC("python_udfs.data_transforms.add_sales_metrics", price_variants);
```

### End-to-End Example
See [`examples/udf_examples/pipelines/customer_text_processing.sf`](../../../examples/udf_examples/pipelines/customer_text_processing.sf) and [`examples/udf_examples/pipelines/sales_analysis.sf`](../../../examples/udf_examples/pipelines/sales_analysis.sf) for real pipelines using UDFs.

---

## 4. CLI for UDFs
- List UDFs: `sqlflow udf list`
- Show UDF info: `sqlflow udf info python_udfs.text_utils.capitalize_words`
- Validate UDFs: `sqlflow udf validate`

---

## 5. Troubleshooting
- **UDF Not Found:** Always use the fully qualified module name in SQL queries (`python_udfs.module.function_name`).
- **Module Not Found:** Ensure your `python_udfs` directory is properly located in your project directory.
- **Import Errors:** Check if your UDF file has the correct imports (pandas, numpy, etc.) and that they're installed.
- **Table UDF Errors:** Make sure table UDFs:
   - Accept a pandas DataFrame as the first argument
   - Return a pandas DataFrame
   - Have all required parameters properly typed
- **Path Issues:** When running demo pipelines, make sure you run commands from the project root directory.

---

## 6. Performance & Best Practices
- **Scalar UDFs** process data row-by-row and may be slower for large datasets.
- **Table UDFs** process entire DataFrames at once and can use vectorized pandas operations for better performance.
- Always use explicit type hints and docstrings.
- For heavy data transformations, prefer table UDFs over scalar UDFs.
- Add proper error handling for None/null values and type conversions (e.g., Decimal to float).
- Consider creating specialized versions of UDFs with default parameters for common use cases.
- Test UDFs independently before pipeline integration.

---

## 7. Available UDF Examples in SQLFlow

### Scalar UDFs
SQLFlow provides examples of scalar UDFs for various tasks:

- Text utilities (`text_utils.py`):
  - `capitalize_words(text: str) -> str`: Capitalizes each word in a string
  - `extract_domain(email: str) -> str`: Extracts domain from an email address
  - `count_words(text: str) -> int`: Counts the number of words in a text
  - `is_valid_email(email: str) -> bool`: Validates if a string is a properly formatted email

- Data transforms (`data_transforms.py`):
  - `calculate_tax(price: float, tax_rate: float = 0.1) -> float`: Calculates price with tax
  - `calculate_tax_default(price: float) -> float`: Specialized version with fixed 10% tax rate
  - `apply_discount(price: float, discount_percent: float) -> float`: Applies percentage discount

### Table UDFs
SQLFlow provides examples of table UDFs that process entire DataFrames:

- `add_sales_metrics(df: pd.DataFrame) -> pd.DataFrame`: Adds calculated columns to a sales DataFrame (total, tax, final_price)
- `detect_outliers(df: pd.DataFrame, column_name: str = "price") -> pd.DataFrame`: Identifies outliers using Z-score method

See [`examples/udf_examples/python_udfs/`](../../../examples/udf_examples/python_udfs/) for complete implementation examples.

---

For further questions or to contribute improvements, see the main [README](../../../README.md) or open an issue.
