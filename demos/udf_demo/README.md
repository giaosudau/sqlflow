# SQLFlow Python UDFs Demo

This demo showcases the Python User-Defined Functions (UDFs) feature in SQLFlow. It demonstrates how to create and use both scalar and table UDFs in SQLFlow pipelines.

## Directory Structure

```
udf_demo/
├── data/                    # Sample data files
│   ├── customers.csv        # Customer information
│   └── sales.csv            # Sales transactions
├── pipelines/               # SQLFlow pipeline files
│   ├── customer_text_processing.sf  # Pipeline using scalar UDFs
│   └── sales_analysis.sf    # Pipeline using both scalar and table UDFs
├── profiles/                # Environment configurations
│   └── dev.yml              # Development environment settings
└── python_udfs/             # Python UDF modules
    ├── text_utils.py        # Text processing UDFs
    └── data_transforms.py   # Data transformation UDFs
```

## UDFs Implemented

### Scalar UDFs (Text Utils)

1. `capitalize_words`: Capitalizes each word in a string
2. `extract_domain`: Extracts domain from an email address
3. `count_words`: Counts the number of words in a text

### Scalar UDFs (Data Transforms)

1. `calculate_tax`: Calculates price with tax added
2. `apply_discount`: Applies a percentage discount to a price

### Table UDFs

1. `add_sales_metrics`: Adds calculated columns to a sales DataFrame (total, tax, final_price)

## Running the Demo

```bash
# Navigate to the SQLFlow project root
cd sqlflow

# List available UDFs
sqlflow udf list

# Get detailed info about a specific UDF
sqlflow udf info text_utils.capitalize_words

# Run the customer text processing pipeline
sqlflow pipeline run customer_text_processing --profile dev

# Run the sales analysis pipeline
sqlflow pipeline run sales_analysis --profile dev
```

## Sample Output

After running the pipelines, check the output directory for generated CSV files:

- `processed_customers_udf_demo_run.csv`: Customers with processed text fields
- `domain_summary_udf_demo_run.csv`: Summary of customer email domains
- `sales_metrics_udf_demo_run.csv`: Sales data with calculated metrics
- `customer_summary_udf_demo_run.csv`: Customer sales summary

## Extending the Demo

To add your own UDFs:

1. Create a new Python file in the `python_udfs` directory
2. Define functions and decorate them with `@python_scalar_udf` or `@python_table_udf`
3. Use the UDFs in your SQLFlow pipelines with `PYTHON_FUNC("module.function", args...)` 