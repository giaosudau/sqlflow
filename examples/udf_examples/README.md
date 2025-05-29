# 🐍 SQLFlow Python UDF Examples

Welcome to the comprehensive Python UDF examples! This directory showcases the full power of SQLFlow's Python UDF system with **26+ discoverable UDFs**.

## 🚀 Quick Start

**Want to see everything in action?** Run our interactive showcase:

```bash
./showcase_python_udfs.sh
```

This will demonstrate:
- 📚 **Scalar UDFs** - Text Processing & Data Quality (Production Ready)
- 🧮 **Advanced Analytics** - Financial calculations & Statistical analysis  
- 🔄 **Table UDF Alternatives** - External processing & Scalar UDF chains
- 🚀 **External Processing** - Unlimited Python Power with pandas integration

## ⚠️ **Important: Table UDF Limitations**

**Current Status**: Table UDFs cannot be called directly in SQL FROM clauses due to DuckDB Python API limitations.

### ✅ **What Works:**
- **Scalar UDFs** - Full SQL integration (`PYTHON_FUNC(...)`)
- **External Processing** - Fetch → Process with pandas → Register back
- **Programmatic Table UDFs** - Direct Python function calls
- **Scalar UDF Chains** - Break complex operations into steps

### ❌ **What Doesn't Work:**
```sql
-- ❌ This doesn't work (DuckDB limitation):
SELECT * FROM my_table_udf(SELECT * FROM source_table);

-- ✅ Use this instead:
-- External Processing or Scalar UDF chains
```

## 📁 Directory Structure

```
udf_examples/
├── 🎮 showcase_python_udfs.sh          # Interactive showcase (START HERE!)
├── 🐍 python_udfs/                     # UDF function definitions (26+ UDFs)
│   ├── text_utils.py                   # Text processing functions
│   ├── data_transforms.py               # Table UDF functions (programmatic)
│   ├── tax_functions.py                 # Tax calculation functions
│   ├── tax_utils.py                     # Tax utility functions
│   └── table_udf_alternatives.py       # Scalar UDF alternatives
├── 📊 pipelines/                        # SQLFlow pipeline examples
│   ├── customer_text_processing.sf     # Text processing pipeline
│   ├── data_quality_check.sf           # Data quality pipeline
│   └── table_udf_alternatives.sf       # Advanced analytics pipeline
├── 📈 data/                             # Sample data files
│   ├── customers.csv                    # Customer data
│   └── sales.csv                        # Sales data
├── 🔧 demonstrate_table_udf_alternatives.py  # External processing demo
└── 📤 output/                           # Generated results (created when you run demos)
```

## 🎯 What You'll Learn

### 📚 **Scalar UDFs (Production Ready)**
Learn how to build production-ready scalar UDFs:
- Format names and text properly
- Extract domains from email addresses
- Count words and analyze text
- Validate email formats and data ranges
- Calculate composite data quality scores

**Example Functions:**
- `capitalize_words()` - Proper name formatting
- `extract_domain()` - Email domain extraction  
- `validate_email_format()` - Email validation
- `calculate_data_quality_score()` - Quality scoring

### 🧮 **Advanced Analytics & Calculations**
Master scalar UDF techniques for:
- Financial calculations (totals, taxes, final prices)
- Statistical analysis (Z-scores, percentiles, outliers)
- Time series analysis (running totals, growth rates)
- Customer segmentation and intelligence

**Example Functions:**
- `calculate_sales_total()` - Financial calculations
- `calculate_z_score()` - Statistical analysis
- `is_outlier()` - Anomaly detection
- `calculate_growth_rate()` - Time series analysis

### 🔄 **Table UDF Alternatives (Workarounds)**
Discover powerful approaches to overcome table UDF limitations:
- **External Processing** - Unlimited pandas/numpy power
- **Scalar UDF Chains** - Break complex operations into steps
- **Hybrid Approaches** - Best of SQL and Python

### 🚀 **External Processing (Recommended for Complex Transformations)**
Unlock unlimited Python capabilities:
- Fetch data from DuckDB → Process with pandas → Register back
- Use any Python library (scikit-learn, numpy, scipy, etc.)
- Perfect for machine learning and complex transformations

## 🎮 Interactive Commands

Explore and manage your UDFs:

```bash
# List all available UDFs (26+ discovered)
sqlflow udf list

# Get detailed info about a specific UDF  
sqlflow udf info python_udfs.text_utils.capitalize_words

# Validate all UDFs
sqlflow udf validate

# Run a specific pipeline
sqlflow pipeline run customer_text_processing --vars '{"run_id": "test", "output_dir": "output"}'
```

## 📊 Example Pipelines

### **Text Processing Pipeline**
```bash
sqlflow pipeline run customer_text_processing --vars '{"run_id": "demo", "output_dir": "output"}'
```
**Demonstrates:** Name formatting, domain extraction, word counting, email validation

### **Data Quality Pipeline**  
```bash
sqlflow pipeline run data_quality_check --vars '{"run_id": "demo", "output_dir": "output"}'
```
**Demonstrates:** Email validation, price range checking, quality scoring

### **Advanced Analytics Pipeline (Scalar UDF Chain)**
```bash
sqlflow pipeline run table_udf_alternatives --vars '{"run_id": "demo", "output_dir": "output"}'
```
**Demonstrates:** Financial calculations, statistical analysis, outlier detection

### **External Processing Demo**
```bash
python demonstrate_table_udf_alternatives.py
```
**Demonstrates:** Pandas integration, external processing, data registration

## 🏗️ Building Your Own UDFs

### Step 1: Create a Scalar UDF Function
```python
# python_udfs/my_functions.py
from sqlflow.udfs import python_scalar_udf

@python_scalar_udf
def my_calculation(value: float, multiplier: float = 2.0) -> float:
    """My custom calculation."""
    if value is None:
        return None
    return value * multiplier
```

### Step 2: Use in SQL Pipeline
```sql
-- my_pipeline.sf
SELECT 
  id,
  PYTHON_FUNC("python_udfs.my_functions.my_calculation", price, 1.5) AS calculated_price
FROM my_table;
```

### Step 3: Run Your Pipeline
```bash
sqlflow pipeline run my_pipeline --vars '{"run_id": "test", "output_dir": "output"}'
```

### Step 4: For Complex Transformations (External Processing)
```python
# For complex table-like operations
def complex_analytics(df: pd.DataFrame) -> pd.DataFrame:
    """Complex analytics using full pandas functionality."""
    result = df.copy()
    
    # Use any pandas/numpy operations
    result["total"] = result["price"] * result["quantity"]
    result["z_score"] = (result["price"] - result["price"].mean()) / result["price"].std()
    result["is_outlier"] = np.abs(result["z_score"]) > 3
    
    return result

# Usage:
# 1. df = engine.execute_query("SELECT * FROM my_table").fetchdf()
# 2. processed_df = complex_analytics(df)
# 3. engine.connection.register("processed_table", processed_df)
```

## 🎯 Real-World Use Cases

The examples demonstrate patterns for:

- **🏢 E-commerce Analytics** - Customer intelligence, sales metrics, outlier detection
- **📊 Data Quality Monitoring** - Validation rules, quality scoring, cleansing
- **📈 Financial Analysis** - Tax calculations, pricing, growth analysis  
- **🔍 Statistical Analysis** - Z-scores, percentiles, anomaly detection
- **🎯 Customer Segmentation** - Behavioral analysis, scoring, classification

## 🔧 Approaches Comparison

### **When to Use Scalar UDFs:**
- ✅ Simple row-by-row transformations
- ✅ Text processing and validation
- ✅ Mathematical calculations
- ✅ Data type conversions
- ✅ Want pure SQL pipeline approach

### **When to Use External Processing:**
- ✅ Complex data transformations
- ✅ Machine learning preprocessing
- ✅ Need specialized Python libraries
- ✅ Working with large datasets
- ✅ Advanced statistical analysis

### **When to Use Programmatic Table UDFs:**
- ✅ Development and testing
- ✅ Complex schema transformations
- ✅ Python-first workflows
- ✅ Prototyping complex operations

## 🚀 Next Steps

1. **🎮 Run the Showcase:** `./showcase_python_udfs.sh`
2. **📖 Explore the Code:** Check out the `python_udfs/` directory
3. **🔧 Try the Pipelines:** Run individual pipeline examples
4. **🧪 Build Your Own:** Create custom UDFs for your use cases
5. **📚 Read the Docs:** Check `docs/user/reference/python_udfs.md`

## 💡 Tips & Best Practices

### **Scalar UDF Design:**
- ✅ Always use explicit type hints
- ✅ Handle None/null values gracefully
- ✅ Add comprehensive docstrings  
- ✅ Use meaningful parameter names
- ✅ Test UDFs independently before pipeline integration

### **External Processing Tips:**
- 🚀 Use pandas vectorization for performance
- 🚀 Process in chunks for large datasets
- 🚀 Leverage any Python library you need
- 🚀 Perfect for complex transformations

### **Performance Optimization:**
- 🚀 Use external processing for complex transformations
- 🚀 Break complex operations into scalar UDF chains when possible
- 🚀 Leverage SQL window functions with scalar UDFs for analytics

## ⚠️ Important Limitations

### **Table UDF SQL Limitations:**
```sql
-- ❌ This doesn't work due to DuckDB Python API limitations:
SELECT * FROM my_table_udf(SELECT * FROM source_table);

-- ✅ Use External Processing instead:
-- 1. df = engine.execute_query("SELECT * FROM source_table").fetchdf()
-- 2. result = my_table_udf_function(df)  
-- 3. engine.connection.register("result_table", result)

-- ✅ Or Scalar UDF Chain:
CREATE TABLE step1 AS SELECT *, PYTHON_FUNC("udf1", col) AS new_col FROM source_table;
CREATE TABLE step2 AS SELECT *, PYTHON_FUNC("udf2", new_col) AS final FROM step1;
```

## 🎉 What's Possible

With these patterns, you can build:

- **📊 Real-time Analytics Dashboards** with live data transformations
- **🤖 ML Data Pipelines** with preprocessing and feature engineering
- **📈 Financial Analysis Tools** with complex calculations and risk metrics
- **🔍 Data Quality Monitors** with validation and cleansing rules
- **🎯 Customer Intelligence Platforms** with segmentation and scoring

**Current Status:** 26+ Python UDFs discovered and fully functional! 🚀

---

*Ready to explore? Start with `./showcase_python_udfs.sh` and dive into the world of Python-powered SQL transformations!* 