# SQLFlow Python UDFs Demo

This demo showcases the Python User-Defined Functions (UDFs) feature in SQLFlow. It demonstrates how to create and use both scalar and table UDFs in SQLFlow pipelines.

## Directory Structure

```
udf_demo/
├── data/                    # Sample data files
│   ├── customers.csv        # Customer information
│   └── sales.csv            # Sales transactions
├── pipelines/               # SQLFlow pipeline files
│   ├── customer_text_processing.sf  # Pipeline using scalar UDFs for text
│   ├── sales_analysis.sf    # Pipeline using both scalar and table UDFs
│   └── data_quality_check.sf # Pipeline for data quality checks
├── profiles/                # Environment configurations
│   └── dev.yml              # Development environment settings
└── python_udfs/             # Python UDF modules
    ├── text_utils.py        # Text processing UDFs
    └── data_transforms.py   # Data transformation UDFs
└── test_udf_discovery.py    # Utility script to test UDF discovery
```

## UDFs Implemented

### Scalar UDFs (Text Utils)

1. `capitalize_words`: Capitalizes each word in a string
2. `extract_domain`: Extracts domain from an email address
3. `count_words`: Counts the number of words in a text
4. `is_valid_email`: Validates if a string is a properly formatted email address

### Scalar UDFs (Data Transforms)

1. `calculate_tax`: Calculates price with tax added
2. `apply_discount`: Applies a percentage discount to a price

### Table UDFs

1. `add_sales_metrics`: Adds calculated columns to a sales DataFrame (total, tax, final_price)
2. `detect_outliers`: Identifies outliers in a numeric column using Z-score method

## CLI Commands for UDFs

SQLFlow CLI provides commands to list and inspect UDFs:

```bash
# List all available UDFs
sqlflow udf list

# Get detailed information about a specific UDF
sqlflow udf info python_udfs.text_utils.capitalize_words
```

### Important Notes on UDF References

When referencing UDFs in SQL queries, you must use the fully qualified module name:

```sql
-- Correct:
PYTHON_FUNC("python_udfs.text_utils.capitalize_words", name)

-- Incorrect:
PYTHON_FUNC("text_utils.capitalize_words", name)
```

## Verifying UDF Discovery

The included test script helps verify that UDFs are properly discoverable by SQLFlow:

```bash
# Run the UDF discovery test script
python test_udf_discovery.py
```

## Troubleshooting

### Common Issues

1. **UDF Not Found**: Make sure you're using the fully qualified module name in your SQL queries, including the `python_udfs` prefix.

2. **Python Module Not Found**: Ensure your Python UDF file is in the `python_udfs` directory and contains properly decorated functions.

3. **Pipeline Parsing Errors**: The current implementation may have specific requirements for UDF usage syntax. Check the examples in the pipeline files.

4. **DuckDB Integration**: If you encounter errors with DuckDB registration, try simplifying your UDF arguments and return types.

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