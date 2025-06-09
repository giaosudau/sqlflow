-- Analyze order data from Google Sheets
CREATE TABLE order_summary AS
SELECT 
    DATE_TRUNC('month', CAST(order_date AS DATE)) as order_month,
    customer_id,
    COUNT(*) as total_orders,
    SUM(CAST(amount AS DECIMAL(10,2))) as total_amount,
    AVG(CAST(amount AS DECIMAL(10,2))) as avg_order_value,
    MIN(CAST(order_date AS DATE)) as first_order,
    MAX(CAST(order_date AS DATE)) as last_order
FROM google_sheets_orders
WHERE order_date IS NOT NULL 
  AND amount IS NOT NULL
  AND CAST(amount AS DECIMAL(10,2)) > 0
GROUP BY 
    DATE_TRUNC('month', CAST(order_date AS DATE)),
    customer_id
ORDER BY order_month DESC, total_amount DESC; 