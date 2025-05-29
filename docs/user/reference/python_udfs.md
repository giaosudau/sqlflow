<!-- filepath: /Users/chanhle/ai-playground/sqlflow/docs/user/reference/python_udfs.md -->
# Python UDFs in SQLFlow: Complete Guide & Showcase

## 🚀 Quick Start

**Want to see Python UDFs in action?** Run our comprehensive showcase:

```bash
cd examples/udf_examples
./showcase_python_udfs.sh
```

This will demonstrate all Python UDF capabilities from simple text processing to advanced analytics!

---

## 🎯 What You Can Build

SQLFlow's Python UDF system enables powerful data transformations directly in your SQL pipelines:

### 📚 **Scalar UDFs - Production Ready**
- Name formatting, domain extraction, word counting
- Email validation, range checking, quality scoring
- Financial calculations (totals, taxes, final prices)
- Statistical analysis (Z-scores, percentiles, outliers)
- Data cleansing and standardization

### 🔄 **Table-Like Transformations**
⚠️ **Important**: Direct table UDF calls in SQL FROM clauses are not supported due to DuckDB Python API limitations. 

**Recommended Approaches:**
- **External Processing** - Fetch → Process with pandas → Register back
- **Scalar UDF Chains** - Break complex operations into manageable steps
- **Hybrid Workflows** - Combine SQL analytics with Python power

### 🏢 **Real-World Use Cases**
- E-commerce analytics and customer intelligence
- Data quality monitoring and validation
- Statistical analysis and outlier detection
- Customer segmentation and behavioral analysis

---

## 🛠️ Three Powerful Approaches

### **Approach 1: Scalar UDFs** ⚡ (Fully Supported)
Perfect for row-by-row transformations and calculations.

  ```python
  from sqlflow.udfs import python_scalar_udf

  @python_scalar_udf
  def capitalize_words(text: str) -> str:
      """Capitalize each word in a string."""
      if text is None:
          return None
      return " ".join(word.capitalize() for word in text.split())
  ```

**Use in SQL:**
```sql
SELECT
  id,
  PYTHON_FUNC("python_udfs.text_utils.capitalize_words", name) AS formatted_name,
  PYTHON_FUNC("python_udfs.text_utils.extract_domain", email) AS email_domain
FROM customers;
```

### **Approach 2: External Processing** 🚀 (Recommended for Complex Transformations)
Unlimited Python power with pandas, numpy, scikit-learn, and more!

  ```python
def add_sales_metrics_external(df: pd.DataFrame) -> pd.DataFrame:
    """Add sales metrics using full pandas functionality."""
      result = df.copy()
      
    # Calculate metrics with full pandas power
      result["total"] = result["price"] * result["quantity"]
      result["tax"] = result["total"] * 0.1
      result["final_price"] = result["total"] + result["tax"]
    
    # Advanced analytics
    result["z_score"] = (result["price"] - result["price"].mean()) / result["price"].std()
    result["is_outlier"] = np.abs(result["z_score"]) > 3
    result["percentile"] = result["price"].rank(pct=True) * 100
      
      return result

# Usage in pipeline
sales_df = engine.execute_query("SELECT * FROM sales").fetchdf()
processed_df = add_sales_metrics_external(sales_df)
engine.connection.register("processed_sales", processed_df)
```

### **Approach 3: Programmatic Table UDFs** 🔧 (Development/Testing)
Table UDFs work when called directly from Python, useful for development and testing:

```python
from sqlflow.udfs import python_table_udf

@python_table_udf(
    output_schema={
        "id": "INTEGER",
        "price": "DOUBLE", 
        "quantity": "INTEGER",
        "total": "DOUBLE",
        "tax": "DOUBLE",
        "final_price": "DOUBLE"
    }
)
def calculate_sales_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate sales metrics - works in Python, not in SQL FROM clauses."""
    result = df.copy()
    result["total"] = result["price"] * result["quantity"]
    result["tax"] = result["total"] * 0.1
    result["final_price"] = result["total"] + result["tax"]
    return result

# Works: Direct Python call
processed_data = calculate_sales_metrics(sales_df)

# Doesn't work: SQL FROM clause (DuckDB limitation)
# SELECT * FROM calculate_sales_metrics(SELECT * FROM sales)  ❌
```

---

## 📖 Step-by-Step Tutorial

### Step 1: Create Your First UDF

Create a file `python_udfs/my_functions.py`:

```python
from sqlflow.udfs import python_scalar_udf

@python_scalar_udf
def calculate_discount(price: float, discount_percent: float = 10.0) -> float:
    """Calculate discounted price."""
    if price is None:
        return None
    return price * (1 - discount_percent / 100)
```

### Step 2: Use in SQL Pipeline

Create a pipeline `my_pipeline.sf`:

```sql
-- Load your data
SOURCE products TYPE CSV PARAMS {
  "path": "data/products.csv",
  "has_header": true
};

LOAD raw_products FROM products;

-- Apply your UDF
CREATE TABLE discounted_products AS
SELECT
  id,
  name,
  price,
  PYTHON_FUNC("python_udfs.my_functions.calculate_discount", price, 15.0) AS sale_price
FROM raw_products;

-- Export results
EXPORT
  SELECT * FROM discounted_products
TO "output/sale_products.csv"
TYPE CSV
OPTIONS { "header": true };
```

### Step 3: Run Your Pipeline

```bash
sqlflow pipeline run my_pipeline --vars '{"run_id": "test", "output_dir": "output"}'
```

---

## 🔧 Advanced Techniques

### **Scalar UDF Chains** 
Break complex table operations into manageable steps:

```sql
-- Step 1: Calculate totals
CREATE TABLE sales_with_totals AS
SELECT *, 
  PYTHON_FUNC("python_udfs.table_udf_alternatives.calculate_sales_total", price, quantity) AS total
FROM raw_sales;

-- Step 2: Add tax calculation  
CREATE TABLE sales_with_tax AS
SELECT *, 
  PYTHON_FUNC("python_udfs.table_udf_alternatives.calculate_sales_tax", total) AS tax
FROM sales_with_totals;

-- Step 3: Statistical analysis
CREATE TABLE sales_with_analytics AS
SELECT s.*,
  PYTHON_FUNC("python_udfs.table_udf_alternatives.calculate_z_score", 
    s.price, stat.mean_price, stat.std_price) AS z_score
FROM sales_with_tax s
CROSS JOIN price_statistics stat;
```

### **External Processing Workflow**

```python
# 1. Fetch data from DuckDB
sales_df = engine.execute_query("SELECT * FROM sales").fetchdf()

# 2. Process with pandas (unlimited functionality!)
processed_df = add_sales_metrics_external(sales_df)
processed_df = detect_outliers_external(processed_df, "price")

# 3. Register back with DuckDB
engine.connection.register("processed_sales", processed_df)

# 4. Continue with SQL
result = engine.execute_query("""
    SELECT customer_id, COUNT(*) as outlier_count
    FROM processed_sales 
    WHERE is_outlier = true
    GROUP BY customer_id
""")
  ```

### **Enhanced UDF Discovery System**

SQLFlow now discovers and manages 26+ Python UDFs automatically:

```bash
# List all available UDFs (26 discovered)
sqlflow udf list

# Get detailed information about any UDF
sqlflow udf info python_udfs.text_utils.capitalize_words

# Validate all UDFs in your project
sqlflow udf validate
```

---

## 🎮 Interactive Commands

Explore and manage your UDFs with these commands:

```bash
# List all available UDFs
sqlflow udf list

# Get detailed info about a specific UDF
sqlflow udf info python_udfs.text_utils.capitalize_words

# Validate all UDFs in your project
sqlflow udf validate

# Validate a specific pipeline
sqlflow pipeline validate my_pipeline

# Run a pipeline
sqlflow pipeline run my_pipeline --vars '{"run_id": "test"}'
```

---

## 📊 Complete Examples

### **Text Processing Pipeline**
```sql
-- Customer text processing with scalar UDFs
SELECT
  id,
  PYTHON_FUNC("python_udfs.text_utils.capitalize_words", name) AS formatted_name,
  PYTHON_FUNC("python_udfs.text_utils.extract_domain", email) AS email_domain,
  PYTHON_FUNC("python_udfs.text_utils.count_words", notes) AS note_word_count,
  PYTHON_FUNC("python_udfs.table_udf_alternatives.validate_email_format", email) AS email_valid
FROM customers;
```

### **Advanced Analytics Pipeline**
```sql
-- Financial calculations with outlier detection
CREATE TABLE comprehensive_analysis AS
SELECT
  s.id,
  s.customer_id,
  s.product,
  s.price,
  s.quantity,
  -- Financial calculations
  PYTHON_FUNC("python_udfs.table_udf_alternatives.calculate_sales_total", s.price, s.quantity) AS total,
  PYTHON_FUNC("python_udfs.table_udf_alternatives.calculate_sales_tax", total) AS tax,
  PYTHON_FUNC("python_udfs.table_udf_alternatives.calculate_final_price", total, tax) AS final_price,
  -- Statistical analysis
  PYTHON_FUNC("python_udfs.table_udf_alternatives.calculate_z_score", s.price, stat.mean_price, stat.std_price) AS z_score,
  PYTHON_FUNC("python_udfs.table_udf_alternatives.is_outlier", z_score) AS is_outlier
FROM sales s
CROSS JOIN price_statistics stat;
```

---

## 🏗️ Best Practices

### **Function Design**
- ✅ Always use explicit type hints
- ✅ Handle None/null values gracefully  
- ✅ Add comprehensive docstrings
- ✅ Use meaningful parameter names
- ✅ Keep functions focused (single responsibility)

### **Performance Tips**
- 🚀 Use external processing for complex transformations
- 🚀 Leverage pandas vectorization over row-by-row operations
- 🚀 Break complex operations into scalar UDF chains
- 🚀 Use SQL window functions with UDFs for analytics

### **Error Handling**
```python
@python_scalar_udf
def safe_divide(numerator: float, denominator: float) -> Optional[float]:
    """Safely divide two numbers."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator
```

---

## 🎯 When to Use Each Approach

### **Use Scalar UDFs When:**
- Simple row-by-row transformations
- Text processing and validation
- Mathematical calculations
- Data type conversions
- Want pure SQL pipeline approach

### **Use External Processing When:**
- Complex data transformations
- Machine learning preprocessing
- Need specialized Python libraries
- Working with large datasets
- Advanced statistical analysis

### **Use Programmatic Table UDFs When:**
- Development and testing
- Complex schema transformations
- Python-first workflows
- Need full pandas DataFrame operations

---

## ⚠️ Important Limitations

### **Table UDF SQL Limitations**
Due to DuckDB Python API constraints:

```sql
-- ❌ This doesn't work:
SELECT * FROM my_table_udf(SELECT * FROM source_table);

-- ✅ Use this instead (External Processing):
-- 1. df = engine.execute_query("SELECT * FROM source_table").fetchdf()
-- 2. result = my_table_udf_function(df)  
-- 3. engine.connection.register("result_table", result)

-- ✅ Or this (Scalar UDF Chain):
CREATE TABLE step1 AS SELECT *, PYTHON_FUNC("my_udf", col1) AS new_col FROM source_table;
CREATE TABLE step2 AS SELECT *, PYTHON_FUNC("other_udf", new_col) AS final_col FROM step1;
```

### **Schema Validation**
Table UDFs require exact schema matching:
```python
# Schema must match exactly what the function returns
@python_table_udf(
    output_schema={
        "id": "INTEGER",
        "result": "DOUBLE"  # Must match actual output columns
    }
)
```

---

## 🚀 Ready to Get Started?

1. **🎮 Try the Showcase:** `./showcase_python_udfs.sh`
2. **📖 Explore Examples:** Check `examples/udf_examples/python_udfs/`
3. **🔧 Build Your Own:** Start with the step-by-step tutorial above
4. **📚 Learn More:** Read the advanced techniques section

### **Quick Commands to Remember:**
```bash
sqlflow udf list                    # See all 26+ available UDFs
sqlflow pipeline validate <name>    # Validate your pipelines  
sqlflow pipeline run <name>         # Execute your transformations
```

---

## 🎉 What's Possible

With SQLFlow's Python UDF system, you can build:

- **📊 Real-time Analytics Dashboards** with live data transformations
- **🤖 ML Data Pipelines** with preprocessing and feature engineering
- **📈 Financial Analysis Tools** with complex calculations and risk metrics
- **🔍 Data Quality Monitors** with validation and cleansing rules
- **🎯 Customer Intelligence Platforms** with segmentation and scoring

**Current Status**: 26 Python UDFs discovered and fully functional! 🚀

---

*For more examples and advanced patterns, explore the `examples/udf_examples/` directory and run the comprehensive showcase.*
