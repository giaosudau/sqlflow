# Building Analytics Pipelines with SQLFlow

**Problem**: "I need to build dashboards, reports, and analytics but setting up data pipelines is complex and time-consuming."

**Solution**: SQLFlow's problem-focused approach gets you from raw data to business insights fast, using SQL you already know.

## 🎯 What You'll Learn

This guide shows you how to solve real analytics problems:
- ✅ **Customer Analytics**: Segmentation, lifetime value, churn analysis
- ✅ **Sales Analytics**: Revenue trends, product performance, forecasting
- ✅ **Data Quality Monitoring**: Automated checks and alerts
- ✅ **Dashboard-Ready Data**: Structured outputs for BI tools
- ✅ **Automated Reporting**: Scheduled exports and notifications

**Complete examples**: All code in this guide comes from working examples in [`/examples/`](../../examples/).

## 🚀 Quick Start: Your First Analytics Pipeline

### Problem: "I need customer analytics for my business dashboard"

**2-minute solution:**

```bash
# Create analytics project
sqlflow init customer_analytics
cd customer_analytics

# Run ready-made customer analytics
sqlflow pipeline run customer_analytics

# See business-ready results
ls output/
head output/customer_summary.csv
```

**What you get:**
- Customer segmentation by country and tier
- Revenue analysis and trends
- Top customer identification
- Export-ready CSV files for dashboards

## 📊 Common Analytics Use Cases

### 1. Customer Analytics Pipeline

**Problem**: "I need to understand my customers better - who are my best customers, where are they located, and what drives revenue?"

**Complete example**: [`/examples/shopify_ecommerce_analytics/`](../../examples/shopify_ecommerce_analytics/)

```sql
-- Customer Analytics Pipeline
-- File: pipelines/customer_analytics.sf

-- Load customer data
SOURCE customers_csv TYPE CSV PARAMS {
  "path": "data/customers.csv",
  "has_header": true
};

LOAD customers FROM customers_csv;

-- Load order data  
SOURCE orders_csv TYPE CSV PARAMS {
  "path": "data/orders.csv",
  "has_header": true
};

LOAD orders FROM orders_csv;

-- Customer segmentation and metrics
CREATE TABLE customer_analytics AS
SELECT 
    c.customer_id,
    c.name,
    c.country,
    c.tier,
    c.age,
    COUNT(o.order_id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as lifetime_value,
    COALESCE(AVG(o.total_amount), 0) as avg_order_value,
    MAX(o.order_date) as last_order_date,
    CASE 
        WHEN MAX(o.order_date) >= CURRENT_DATE - INTERVAL '30 days' THEN 'Active'
        WHEN MAX(o.order_date) >= CURRENT_DATE - INTERVAL '90 days' THEN 'At Risk'
        ELSE 'Churned'
    END as customer_status
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.country, c.tier, c.age;

-- Regional performance summary
CREATE TABLE regional_summary AS
SELECT 
    country,
    tier,
    COUNT(*) as customer_count,
    AVG(age) as avg_customer_age,
    SUM(total_orders) as total_orders,
    SUM(lifetime_value) as total_revenue,
    AVG(lifetime_value) as avg_lifetime_value,
    AVG(avg_order_value) as avg_order_value
FROM customer_analytics
GROUP BY country, tier
ORDER BY total_revenue DESC;

-- Export for dashboards
EXPORT customer_analytics TO 'output/customer_analytics.csv' TYPE CSV;
EXPORT regional_summary TO 'output/regional_summary.csv' TYPE CSV;
```

**Business value:**
- Identify high-value customer segments
- Track customer lifecycle and churn risk
- Regional performance analysis for marketing
- Ready for Tableau, Power BI, or any BI tool

### 2. Sales Analytics Pipeline

**Problem**: "I need to track sales performance, identify trends, and forecast revenue."

**Complete example**: [`/examples/transform_layer_demo/`](../../examples/transform_layer_demo/)

```sql
-- Sales Analytics Pipeline  
-- File: pipelines/sales_analytics.sf

-- Load products and orders (assuming already loaded)

-- Monthly sales trends
CREATE TABLE monthly_sales AS
SELECT 
    DATE_TRUNC('month', order_date) as month,
    COUNT(*) as order_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    SUM(quantity) as total_units_sold
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;

-- Product performance analysis
CREATE TABLE product_performance AS
SELECT 
    p.product_id,
    p.name as product_name,
    p.category,
    p.price,
    COUNT(o.order_id) as times_ordered,
    SUM(o.quantity) as total_quantity_sold,
    SUM(o.total_amount) as total_revenue,
    AVG(o.quantity) as avg_quantity_per_order,
    MAX(o.order_date) as last_ordered_date
FROM products p
LEFT JOIN orders o ON p.product_id = o.product_id
GROUP BY p.product_id, p.name, p.category, p.price
ORDER BY total_revenue DESC;

-- Top performing categories
CREATE TABLE category_performance AS  
SELECT 
    category,
    COUNT(DISTINCT product_id) as product_count,
    SUM(total_revenue) as category_revenue,
    AVG(total_revenue) as avg_product_revenue,
    SUM(total_quantity_sold) as total_units_sold
FROM product_performance
GROUP BY category
ORDER BY category_revenue DESC;

-- Sales forecasting (simple trend)
CREATE TABLE sales_forecast AS
WITH monthly_growth AS (
    SELECT 
        month,
        total_revenue,
        LAG(total_revenue) OVER (ORDER BY month) as prev_month_revenue,
        (total_revenue - LAG(total_revenue) OVER (ORDER BY month)) / 
        LAG(total_revenue) OVER (ORDER BY month) * 100 as growth_rate
    FROM monthly_sales
)
SELECT 
    month,
    total_revenue,
    growth_rate,
    AVG(growth_rate) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_growth_rate
FROM monthly_growth
ORDER BY month;

-- Export results
EXPORT monthly_sales TO 'output/monthly_sales.csv' TYPE CSV;
EXPORT product_performance TO 'output/product_performance.csv' TYPE CSV;
EXPORT category_performance TO 'output/category_performance.csv' TYPE CSV;
EXPORT sales_forecast TO 'output/sales_forecast.csv' TYPE CSV;
```

**Business value:**
- Track revenue trends and seasonality  
- Identify best and worst performing products
- Category analysis for inventory planning
- Simple forecasting for budget planning

### 3. Data Quality Monitoring Pipeline

**Problem**: "I need automated data quality checks to ensure my analytics are reliable."

**Complete example**: [`/examples/conditional_pipelines/`](../../examples/conditional_pipelines/) (data quality checks)

```sql
-- Data Quality Monitoring Pipeline
-- File: pipelines/data_quality_monitoring.sf

-- Completeness checks
CREATE TABLE data_completeness AS
SELECT 
    'customers' as table_name,
    COUNT(*) as total_records,
    COUNT(customer_id) as non_null_ids,
    COUNT(name) as non_null_names,
    COUNT(email) as non_null_emails,
    COUNT(country) as non_null_countries,
    ROUND(COUNT(email) * 100.0 / COUNT(*), 2) as email_completeness_pct,
    ROUND(COUNT(country) * 100.0 / COUNT(*), 2) as country_completeness_pct
FROM customers

UNION ALL

SELECT 
    'orders' as table_name,
    COUNT(*) as total_records,
    COUNT(order_id) as non_null_ids,
    COUNT(customer_id) as non_null_customer_ids,
    COUNT(total_amount) as non_null_amounts,
    COUNT(order_date) as non_null_dates,
    ROUND(COUNT(customer_id) * 100.0 / COUNT(*), 2) as customer_id_completeness_pct,
    ROUND(COUNT(total_amount) * 100.0 / COUNT(*), 2) as amount_completeness_pct
FROM orders;

-- Duplicate detection
CREATE TABLE duplicate_analysis AS
SELECT 
    'customers' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT customer_id) as unique_ids,
    COUNT(*) - COUNT(DISTINCT customer_id) as duplicate_count,
    COUNT(DISTINCT email) as unique_emails,
    COUNT(*) - COUNT(DISTINCT email) as duplicate_emails
FROM customers

UNION ALL

SELECT 
    'orders' as table_name,
    COUNT(*) as total_records, 
    COUNT(DISTINCT order_id) as unique_ids,
    COUNT(*) - COUNT(DISTINCT order_id) as duplicate_count,
    NULL as unique_emails,
    NULL as duplicate_emails
FROM orders;

-- Data freshness and ranges
CREATE TABLE data_ranges AS
SELECT 
    'customers' as table_name,
    MIN(age) as min_age,
    MAX(age) as max_age,
    AVG(age) as avg_age,
    NULL as min_date,
    NULL as max_date,
    NULL as date_range_days
FROM customers

UNION ALL

SELECT 
    'orders' as table_name,
    MIN(total_amount) as min_age,
    MAX(total_amount) as max_age,
    AVG(total_amount) as avg_age,
    MIN(order_date) as min_date,
    MAX(order_date) as max_date,
    DATE_DIFF('day', MIN(order_date), MAX(order_date)) as date_range_days
FROM orders;

-- Outlier detection
CREATE TABLE outlier_analysis AS
WITH order_stats AS (
    SELECT 
        AVG(total_amount) as avg_amount,
        STDDEV(total_amount) as stddev_amount
    FROM orders
)
SELECT 
    o.order_id,
    o.total_amount,
    s.avg_amount,
    ABS(o.total_amount - s.avg_amount) / s.stddev_amount as z_score,
    CASE 
        WHEN ABS(o.total_amount - s.avg_amount) / s.stddev_amount > 3 THEN 'Outlier'
        WHEN ABS(o.total_amount - s.avg_amount) / s.stddev_amount > 2 THEN 'Unusual'
        ELSE 'Normal'
    END as outlier_status
FROM orders o
CROSS JOIN order_stats s
WHERE ABS(o.total_amount - s.avg_amount) / s.stddev_amount > 2
ORDER BY z_score DESC;

-- Export quality reports
EXPORT data_completeness TO 'output/data_completeness.csv' TYPE CSV;
EXPORT duplicate_analysis TO 'output/duplicate_analysis.csv' TYPE CSV;
EXPORT data_ranges TO 'output/data_ranges.csv' TYPE CSV;
EXPORT outlier_analysis TO 'output/outlier_analysis.csv' TYPE CSV;
```

**Business value:**
- Automated data quality monitoring
- Early detection of data issues
- Trust and confidence in analytics
- Compliance and audit readiness

## 🔧 Advanced Analytics Patterns

### 1. Cohort Analysis

**Problem**: "I need to understand customer retention and behavior over time."

```sql
-- Customer cohort analysis
CREATE TABLE customer_cohorts AS
WITH first_orders AS (
    SELECT 
        customer_id,
        MIN(DATE_TRUNC('month', order_date)) as cohort_month
    FROM orders
    GROUP BY customer_id
),
monthly_activity AS (
    SELECT 
        o.customer_id,
        fo.cohort_month,
        DATE_TRUNC('month', o.order_date) as activity_month,
        DATE_DIFF('month', fo.cohort_month, DATE_TRUNC('month', o.order_date)) as period_number
    FROM orders o
    JOIN first_orders fo ON o.customer_id = fo.customer_id
)
SELECT 
    cohort_month,
    period_number,
    COUNT(DISTINCT customer_id) as customers,
    SUM(COUNT(DISTINCT customer_id)) OVER (PARTITION BY cohort_month ORDER BY period_number ROWS UNBOUNDED PRECEDING) as cumulative_customers
FROM monthly_activity
GROUP BY cohort_month, period_number
ORDER BY cohort_month, period_number;
```

### 2. RFM Analysis (Recency, Frequency, Monetary)

**Problem**: "I need to segment customers based on their purchasing behavior."

```sql
-- RFM customer segmentation
CREATE TABLE rfm_analysis AS
WITH customer_rfm AS (
    SELECT 
        customer_id,
        MAX(order_date) as last_order_date,
        DATE_DIFF('day', MAX(order_date), CURRENT_DATE) as recency_days,
        COUNT(*) as frequency,
        SUM(total_amount) as monetary_value
    FROM orders
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT 
        customer_id,
        recency_days,
        frequency,
        monetary_value,
        NTILE(5) OVER (ORDER BY recency_days DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency) as f_score,
        NTILE(5) OVER (ORDER BY monetary_value) as m_score
    FROM customer_rfm
)
SELECT 
    customer_id,
    recency_days,
    frequency,
    monetary_value,
    r_score,
    f_score,
    m_score,
    CASE 
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 3 AND f_score <= 2 THEN 'New Customers'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost Customers'
        ELSE 'Developing'
    END as customer_segment
FROM rfm_scores;
```

### 3. Time Series Analysis

**Problem**: "I need to analyze trends and seasonality in my data."

```sql
-- Time series analysis with moving averages
CREATE TABLE time_series_analysis AS
WITH daily_sales AS (
    SELECT 
        order_date,
        COUNT(*) as daily_orders,
        SUM(total_amount) as daily_revenue
    FROM orders
    GROUP BY order_date
)
SELECT 
    order_date,
    daily_orders,
    daily_revenue,
    AVG(daily_revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as revenue_7day_ma,
    AVG(daily_revenue) OVER (ORDER BY order_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as revenue_30day_ma,
    (daily_revenue - AVG(daily_revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) / 
     AVG(daily_revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) * 100 as pct_deviation_from_7day_avg
FROM daily_sales
ORDER BY order_date;
```

## 🐍 Adding Python for Advanced Analytics

**Problem**: "SQL is great, but I need Python for complex calculations like machine learning."

**Complete example**: [`/examples/udf_examples/`](../../examples/udf_examples/)

### Customer Scoring with Python UDFs

```python
# python_udfs/customer_scoring.py
from sqlflow.udfs.decorators import python_scalar_udf, python_table_udf
import pandas as pd
import numpy as np

@python_scalar_udf
def calculate_clv_score(recency: int, frequency: int, monetary: float) -> float:
    """Calculate Customer Lifetime Value score using weighted formula."""
    # Normalize inputs and apply weights
    recency_score = max(0, 100 - (recency / 30 * 10))  # Higher score for recent activity
    frequency_score = min(100, frequency * 10)  # Higher score for frequent purchases
    monetary_score = min(100, monetary / 10)  # Higher score for high spending
    
    # Weighted CLV score
    clv_score = (recency_score * 0.3) + (frequency_score * 0.4) + (monetary_score * 0.3)
    return round(clv_score, 2)

@python_table_udf
def advanced_customer_segmentation(df: pd.DataFrame) -> pd.DataFrame:
    """Perform advanced customer segmentation using machine learning-like clustering."""
    result = df.copy()
    
    # Calculate percentiles for RFM
    result['recency_percentile'] = df['recency_days'].rank(pct=True, ascending=False)
    result['frequency_percentile'] = df['frequency'].rank(pct=True)
    result['monetary_percentile'] = df['monetary_value'].rank(pct=True)
    
    # Create composite score
    result['composite_score'] = (
        result['recency_percentile'] * 0.3 +
        result['frequency_percentile'] * 0.4 +
        result['monetary_percentile'] * 0.3
    )
    
    # Segment based on composite score
    result['segment'] = pd.cut(result['composite_score'], 
                              bins=[0, 0.2, 0.4, 0.6, 0.8, 1.0],
                              labels=['Low_Value', 'Developing', 'Core', 'High_Value', 'VIP'])
    
    return result
```

**Use in SQL:**

```sql
-- Enhanced customer analytics with Python UDFs
CREATE TABLE enhanced_customer_analytics AS
SELECT 
    customer_id,
    recency_days,
    frequency,
    monetary_value,
    PYTHON_FUNC("python_udfs.customer_scoring.calculate_clv_score", 
                recency_days, frequency, monetary_value) as clv_score
FROM customer_rfm;

-- Advanced segmentation using Python
CREATE TABLE advanced_segments AS
SELECT * FROM PYTHON_FUNC("python_udfs.customer_scoring.advanced_customer_segmentation", 
                          customer_rfm);
```

## 📈 Dashboard Integration Patterns

### 1. Tableau/Power BI Ready Exports

```sql
-- Dashboard-optimized customer summary
CREATE TABLE dashboard_customer_summary AS
SELECT 
    -- Dimensions for filtering
    country,
    tier,
    customer_segment,
    
    -- Metrics for visualization
    COUNT(*) as customer_count,
    SUM(lifetime_value) as total_ltv,
    AVG(lifetime_value) as avg_ltv,
    AVG(avg_order_value) as avg_aov,
    
    -- Calculated fields for dashboards
    SUM(CASE WHEN customer_status = 'Active' THEN 1 ELSE 0 END) as active_customers,
    SUM(CASE WHEN customer_status = 'At Risk' THEN 1 ELSE 0 END) as at_risk_customers,
    SUM(CASE WHEN customer_status = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
    
    -- Percentages for charts
    ROUND(SUM(CASE WHEN customer_status = 'Active' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as active_pct
FROM customer_analytics
GROUP BY country, tier, customer_segment;

-- Export with dashboard-friendly naming
EXPORT dashboard_customer_summary TO 'output/dashboard_customer_summary.csv' TYPE CSV;
```

### 2. JSON Exports for Web Dashboards

```sql
-- JSON export for web dashboards
CREATE TABLE web_dashboard_data AS
SELECT 
    country,
    json_object(
        'customer_count', customer_count,
        'total_revenue', total_ltv,
        'avg_revenue_per_customer', avg_ltv,
        'active_customers', active_customers,
        'churn_rate', ROUND((churned_customers * 100.0 / customer_count), 2)
    ) as metrics
FROM dashboard_customer_summary;

EXPORT web_dashboard_data TO 'output/web_dashboard_data.json' TYPE JSON;
```

## ⚙️ Environment Configuration

### Development vs Production

**Problem**: "I need different settings for development testing vs production runs."

**Solution**: Use profiles to manage environments easily.

```yaml
# profiles/dev.yml - Fast development
name: "dev"
engine:
  type: "duckdb"
  mode: "memory"  # Fast, temporary
variables:
  data_sample_size: 1000
  enable_expensive_operations: false
  export_format: "csv"

# profiles/production.yml - Reliable production  
name: "production"
engine:
  type: "duckdb"
  mode: "persistent"  # Saves data to disk
  database_path: "/data/analytics.db"
variables:
  data_sample_size: null  # Use all data
  enable_expensive_operations: true
  export_format: "parquet"
```

**Use in pipelines:**

```sql
-- Conditional processing based on environment
{% if vars.enable_expensive_operations %}
-- Run complex ML scoring only in production
CREATE TABLE customer_ml_scores AS
SELECT * FROM PYTHON_FUNC("ml_models.score_customers", customer_analytics);
{% endif %}

-- Sample data in development
CREATE TABLE processed_orders AS
SELECT * FROM orders
{% if vars.data_sample_size %}
LIMIT {{ vars.data_sample_size }}
{% endif %};
```

**Run with different profiles:**

```bash
# Development (fast, in-memory)
sqlflow pipeline run customer_analytics --profile dev

# Production (persistent, full data)
sqlflow pipeline run customer_analytics --profile production
```

## 🚨 Troubleshooting Common Issues

### 1. "Table not found" errors

**Problem**: Pipeline fails because tables don't exist.

**Solution**: Check execution order and dependencies.

```bash
# Validate pipeline before running
sqlflow pipeline validate customer_analytics

# Check which pipelines are available
sqlflow pipeline list

# Run dependencies first
sqlflow pipeline run data_loading
sqlflow pipeline run customer_analytics
```

### 2. Performance issues with large datasets

**Problem**: Pipelines run slowly with large data.

**Solutions**:

```sql
-- Add indexes for better performance
CREATE INDEX idx_customer_id ON orders(customer_id);
CREATE INDEX idx_order_date ON orders(order_date);

-- Use sampling in development
CREATE TABLE sample_orders AS
SELECT * FROM orders
WHERE RANDOM() < 0.1  -- 10% sample
LIMIT 10000;

-- Partition large tables by date
CREATE TABLE partitioned_orders AS
SELECT 
    *,
    DATE_TRUNC('month', order_date) as partition_month
FROM orders;
```

### 3. Memory issues

**Problem**: "Out of memory" errors with large datasets.

**Solutions**:

```bash
# Use persistent mode instead of memory
sqlflow pipeline run customer_analytics --profile production

# Process data in chunks
sqlflow pipeline run customer_analytics --vars '{"chunk_size": 50000}'
```

```sql
-- Process in batches using variables
CREATE TABLE customer_batch AS
SELECT * FROM customer_analytics
WHERE customer_id BETWEEN {{ vars.start_id }} AND {{ vars.end_id }};
```

## 📚 Next Steps

### **Ready to dive deeper?**

1. **Connect Real Data**: [Connecting Data Sources](connecting-data-sources.md)
2. **Add Python Power**: [UDF Examples](../../examples/udf_examples/)
3. **Production Deployment**: [Technical Overview](../developer-guides/technical-overview.md)

### **Explore More Examples**

- **E-commerce Analytics**: [`/examples/shopify_ecommerce_analytics/`](../../examples/shopify_ecommerce_analytics/)
- **Conditional Logic**: [`/examples/conditional_pipelines/`](../../examples/conditional_pipelines/)
- **Incremental Processing**: [`/examples/incremental_loading_demo/`](../../examples/incremental_loading_demo/)

### **Community Resources**

- 💬 **Ask Questions**: [GitHub Discussions](https://github.com/sqlflow/sqlflow/discussions)
- 📖 **Full Documentation**: [Reference Guides](../reference/)
- ⭐ **Star on GitHub**: [github.com/sqlflow/sqlflow](https://github.com/sqlflow/sqlflow)

---

**🎯 Key Takeaway**: SQLFlow transforms the complex data engineering work into simple SQL patterns. Focus on solving business problems, not infrastructure challenges. 