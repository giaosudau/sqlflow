-- Monthly Data Processing Pipeline
-- Demonstrates pattern matching and multiple file handling

-- Load all monthly sales files using pattern matching
SOURCE monthly_sales TYPE PARQUET PARAMS {
    "path": "data/sales_2024_*.parquet",
    "combine_files": true
};

-- Load customer data for enrichment
SOURCE customers TYPE PARQUET PARAMS {
    "path": "data/customers.parquet"
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
    AVG(unit_price) as avg_unit_price,
    MIN(sale_date) as first_sale,
    MAX(sale_date) as last_sale
FROM monthly_sales
GROUP BY 
    EXTRACT(month FROM sale_date),
    DATE_TRUNC('month', sale_date),
    product_name
ORDER BY month_number, product_name;

-- Create customer behavior analysis across months
CREATE TABLE customer_monthly_behavior AS
SELECT 
    ms.customer_id,
    c.customer_name,
    c.segment,
    EXTRACT(month FROM ms.sale_date) as month_number,
    COUNT(*) as purchases_in_month,
    SUM(ms.total_amount) as monthly_spend,
    AVG(ms.total_amount) as avg_purchase_value,
    STRING_AGG(DISTINCT ms.product_name, ', ') as products_purchased
FROM monthly_sales ms
LEFT JOIN customers c ON ms.customer_id = c.customer_id
GROUP BY 
    ms.customer_id, 
    c.customer_name, 
    c.segment,
    EXTRACT(month FROM ms.sale_date)
ORDER BY ms.customer_id, month_number;

-- Create product performance summary
CREATE TABLE product_performance AS
SELECT 
    product_name,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(*) as total_sales,
    SUM(quantity) as total_units_sold,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_sale_value,
    MIN(sale_date) as first_sale_date,
    MAX(sale_date) as last_sale_date,
    COUNT(DISTINCT DATE_TRUNC('month', sale_date)) as months_active
FROM monthly_sales
GROUP BY product_name
ORDER BY total_revenue DESC;

-- Export comprehensive analysis
EXPORT monthly_trends TO CSV "output/monthly_product_trends.csv";
EXPORT customer_monthly_behavior TO CSV "output/customer_monthly_behavior.csv";
EXPORT product_performance TO CSV "output/product_performance_summary.csv"; 