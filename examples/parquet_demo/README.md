# Parquet Connector Demo

This demo showcases the SQLFlow Parquet connector capabilities including schema inference, file pattern matching, and data analytics.

## Overview

The Parquet connector provides high-performance columnar data loading with advanced features:

- **Schema inference** from Parquet metadata
- **Column selection** for optimized data loading  
- **File pattern matching** for bulk operations
- **Multiple file handling** with combination options
- **Incremental loading** support

## Demo Structure

```
parquet_demo/
├── data/                    # Sample Parquet files
│   ├── sales.parquet       # Main sales data
│   ├── sales_2024_01.parquet  # Monthly sales files
│   ├── sales_2024_02.parquet
│   ├── sales_2024_03.parquet
│   └── customers.parquet   # Customer data
├── pipelines/              # SQL pipeline definitions
│   ├── analyze_sales.sql   # Sales analysis pipeline
│   └── process_monthly_data.sql  # Monthly data processing
├── profiles/
│   └── dev.yml            # Database configuration
├── output/                # Generated results
├── create_sample_data.py  # Data generation script
├── run_demo.sh           # Main demo script
└── README.md             # This file
```

## Quick Start

1. **Run the demo:**
   ```bash
   ./run_demo.sh
   ```

2. **View results:**
   ```bash
   # Check generated files
   ls -la output/
   
   # Query the database directly
   sqlflow debug query "SELECT * FROM sales_analysis LIMIT 10" --profile dev
   ```

## Sample Data

The demo creates realistic sample data:

- **Sales data**: 100 transactions with products, customers, dates, and amounts
- **Customer data**: 50 customers with segments and lifetime values
- **Monthly files**: Sales data split by month for pattern matching demos

## Pipeline Examples

### Sales Analysis Pipeline

```sql
-- Load sales data from single Parquet file
SOURCE sales TYPE PARQUET PARAMS {
    "path": "data/sales.parquet"
};

-- Load customer data with column selection
SOURCE customers TYPE PARQUET PARAMS {
    "path": "data/customers.parquet",
    "columns": ["customer_id", "customer_name", "segment", "lifetime_value"]
};

-- Create enriched sales analysis
CREATE TABLE sales_analysis AS
SELECT 
    s.sale_id,
    s.customer_id,
    c.customer_name,
    c.segment as customer_segment,
    s.product_name,
    s.sale_date,
    s.quantity,
    s.unit_price,
    s.total_amount,
    c.lifetime_value
FROM sales s
JOIN customers c ON s.customer_id = c.customer_id;
```

### Monthly Data Processing

```sql
-- Load all monthly sales files using pattern matching
SOURCE monthly_sales TYPE PARQUET PARAMS {
    "path": "data/sales_2024_*.parquet",
    "combine_files": true
};

-- Create comprehensive monthly analysis
CREATE TABLE monthly_trends AS
SELECT 
    EXTRACT(month FROM sale_date) as month_number,
    DATE_TRUNC('month', sale_date) as sale_month,
    product_name,
    COUNT(*) as transaction_count,
    SUM(quantity) as total_quantity,
    SUM(total_amount) as revenue,
    AVG(unit_price) as avg_unit_price
FROM monthly_sales
GROUP BY month_number, sale_month, product_name
ORDER BY month_number, product_name;
```

## Configuration Options

### Basic Usage

```sql
-- Single Parquet file
SOURCE analytics TYPE PARQUET PARAMS {
  "path": "data/analytics.parquet"
};
```

### Column Selection

```sql
-- Read only specific columns for performance
SOURCE metrics TYPE PARQUET PARAMS {
  "path": "data/large_dataset.parquet",
  "columns": ["user_id", "event_time", "metric_value"]
};
```

### File Pattern Matching

```sql
-- Process multiple files with patterns
SOURCE daily_logs TYPE PARQUET PARAMS {
  "path": "logs/daily_*.parquet",
  "combine_files": true
};
```

### Batch Processing

```sql
-- Control memory usage with batch size
SOURCE big_data TYPE PARQUET PARAMS {
  "path": "data/large_file.parquet",
  "batch_size": 5000
};
```

## Performance Tips

1. **Use column selection** when you don't need all columns
2. **Set appropriate batch sizes** for memory management
3. **Combine files** when processing multiple related files
4. **Use file patterns** for efficient bulk operations

## Troubleshooting

### Common Issues

**File not found errors:**
- Check file paths are correct
- Ensure Parquet files exist in the data directory
- Verify file permissions

**Memory issues with large files:**
- Reduce batch_size parameter
- Use column selection to limit data
- Process files separately instead of combining

**Schema mismatches:**
- Ensure all files have compatible schemas when combining
- Check column names and types match expectations

### Debug Commands

```bash
# Test connection
sqlflow debug query "SELECT COUNT(*) FROM sales" --profile dev

# Check schema
sqlflow debug query "DESCRIBE sales_analysis" --profile dev

# View sample data
sqlflow debug query "SELECT * FROM monthly_trends LIMIT 5" --profile dev
```

## Next Steps

- Explore the [Parquet Connector Documentation](../../sqlflow/connectors/parquet/README.md)
- Try modifying the pipelines with your own data
- Experiment with different configuration options
- Check out other connector demos in the examples directory 