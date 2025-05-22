# SQLFlow Python UDFs Demo

This demo showcases the Python User-Defined Functions (UDFs) feature in SQLFlow, which allows you to extend SQL capabilities with custom Python functions. It demonstrates how to create and use both scalar and table UDFs in data processing pipelines.

## Overview

The demo includes:
- Scalar UDFs for text processing and simple calculations
- Table UDFs for data transformation and quality analysis
- Complete pipelines showing real-world UDF usage patterns
- Sample data for customers and sales analysis

## Directory Structure

```
udf_demo/
├── data/                    # Sample data files
│   ├── customers.csv        # Customer information
│   └── sales.csv            # Sales transactions
├── pipelines/               # SQLFlow pipeline files
│   ├── customer_text_processing.sf  # Text processing with scalar UDFs
│   ├── sales_analysis.sf            # Complete pipeline with various UDFs
│   └── data_quality_check.sf        # Data validation with UDFs
├── profiles/                # Environment configurations
│   └── dev.yml              # Development environment settings
└── python_udfs/             # Python UDF modules
    ├── text_utils.py        # Text processing UDFs
    ├── data_transforms.py   # Data transformation UDFs
    ├── tax_utils.py         # Tax calculation UDFs
    ├── tax_functions.py     # Advanced tax-related UDFs
    └── enhanced_udfs.py     # Enhanced UDFs for specialized tasks
└── test_udf_discovery.py    # Utility script to test UDF discovery
```

## Running the Demo

To run the demo pipelines, use the following commands:

```bash
# Text processing pipeline
sqlflow pipeline run customer_text_processing --profile dev --vars '{"run_id": "demo_run", "output_dir": "output"}'

# Sales analysis pipeline
sqlflow pipeline run sales_analysis --profile dev --vars '{"run_id": "demo_run", "output_dir": "output"}'

# Data quality check pipeline
sqlflow pipeline run data_quality_check --profile dev --vars '{"run_id": "demo_run", "output_dir": "output"}'
```

The results will be stored in the specified `output_dir` with filenames that include the `run_id`.

## Available UDFs

### Scalar UDFs

Text utilities (`text_utils.py`):
- `capitalize_words(text: str) -> str`: Capitalizes each word in a string
- `extract_domain(email: str) -> str`: Extracts domain from an email address
- `count_words(text: str) -> int`: Counts the number of words in a text
- `is_valid_email(email: str) -> bool`: Validates if a string is a properly formatted email

Data transforms (`data_transforms.py`):
- `calculate_tax(price: float, tax_rate: float = 0.1) -> float`: Calculates price with tax
- `apply_discount(price: float, discount_percent: float) -> float`: Applies percentage discount

### Table UDFs

The following UDFs process entire DataFrames:

- `add_sales_metrics(df: pd.DataFrame) -> pd.DataFrame`: Adds calculated columns to a sales DataFrame (total, tax, final_price)
- `detect_outliers(df: pd.DataFrame, column_name: str = "price") -> pd.DataFrame`: Identifies outliers using Z-score method

## Using UDFs in SQL

### Scalar UDF Usage

In SQL, scalar UDFs are called as part of expressions:

```sql
SELECT
  id,
  name,
  PYTHON_FUNC("python_udfs.text_utils.capitalize_words", name) AS formatted_name,
  PYTHON_FUNC("python_udfs.text_utils.is_valid_email", email) AS has_valid_email
FROM customers;
```

### Table UDF Usage

Table UDFs are called in the FROM clause to transform entire tables:

```sql
-- Process a table with a table UDF
CREATE TABLE enriched_sales AS
SELECT * FROM PYTHON_FUNC("python_udfs.data_transforms.add_sales_metrics", sales_table);
```

## Exploring UDFs with CLI

SQLFlow provides CLI commands to explore available UDFs:

```bash
# List all available UDFs
sqlflow udf list

# Get detailed information about a specific UDF
sqlflow udf info python_udfs.text_utils.capitalize_words

# Validate UDFs
sqlflow udf validate
```

## Troubleshooting

### Common Issues

1. **UDF Not Found**: Always use the fully qualified module name in SQL queries (`python_udfs.module.function_name`).

2. **Module Not Found**: Ensure your `python_udfs` directory is properly located in your project directory.

3. **Import Errors**: Check if your UDF file has the correct imports (pandas, numpy, etc.) and that they're installed.

4. **Table UDF Errors**: Make sure table UDFs:
   - Accept a pandas DataFrame as the first argument
   - Return a pandas DataFrame
   - Have all required parameters properly typed

5. **Path Issues**: When running the demo, make sure you run the commands from the project root directory.

## Performance Tips

- Scalar UDFs process data row-by-row and may be slower for large datasets.
- Table UDFs process entire DataFrames at once and can use vectorized pandas operations for better performance.
- For best performance, use the appropriate UDF type for your use case:
  - Use scalar UDFs for simple transformations on individual values
  - Use table UDFs for complex operations on entire datasets

## Next Steps

After exploring this demo, you can:
1. Create your own UDFs in the `python_udfs` directory
2. Build custom pipelines using your UDFs
3. Experiment with more complex transformations
4. Integrate with your own data sources

For more information, see the [SQLFlow Python UDFs documentation](../../docs/user/reference/python_udfs.md).

## Verifying UDF Discovery

The included test script helps verify that UDFs are properly discoverable by SQLFlow:

```bash
# Run the UDF discovery test script
python test_udf_discovery.py
```

## Notes on UDF Usage

### Scalar UDFs
Scalar UDFs process one row at a time and are used in standard SQL expressions:

```sql
SELECT 
  customer_id,
  PYTHON_FUNC("python_udfs.text_utils.capitalize_words", name) AS formatted_name
FROM customers;
```

### Table UDFs
Table UDFs process an entire DataFrame and return a DataFrame. They're used in FROM clauses:

```sql
CREATE TABLE sales_with_metrics AS
SELECT * 
FROM PYTHON_FUNC("python_udfs.data_transforms.add_sales_metrics", sales_table);
```

### Performance Considerations
- Scalar UDFs are called once per row, so they may be slower for large datasets
- Table UDFs can leverage vectorized operations in pandas for better performance
- For critical performance needs, consider pre-processing data before using UDFs 