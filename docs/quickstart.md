# SQLFlow 2-Minute Quickstart

**Goal**: See SQLFlow working in under 2 minutes with realistic data and instant results.

## 🚀 What You'll Accomplish

In the next 2 minutes, you'll:
- ✅ Create a complete analytics project with 1,000 customers and 5,000 orders
- ✅ Run a customer analytics pipeline with SQL you already know
- ✅ Generate business-ready reports and summaries
- ✅ Understand why SQLFlow is the fastest way from data to insights

## Step 1: Install SQLFlow (30 seconds)

```bash
# Install SQLFlow (one command, that's it!)
pip install sqlflow-core
```

**Already installed?** Skip to Step 2.

## Step 2: Create Your Analytics Project (30 seconds)

```bash
# Create project with realistic sample data
sqlflow init customer_analytics_demo
cd customer_analytics_demo

# See what was created
ls -la
```

**What just happened?** SQLFlow created:
- 📊 **Sample data**: 1,000 customers, 5,000 orders, 500 products
- 🔄 **Ready pipelines**: Customer analytics, data quality monitoring
- ⚙️ **Configuration**: Profiles for dev/production environments
- 📁 **Project structure**: Everything organized and ready to use

## Step 3: Run Your First Pipeline (60 seconds)

```bash
# Run customer analytics pipeline
sqlflow pipeline run customer_analytics

# See immediate results
ls output/
cat output/customer_summary.csv
```

**Expected output:**
```
customer_summary.csv    - Customer metrics by country and tier
top_customers.csv       - Highest value customers  
monthly_trends.csv      - Order trends by month
```

## Step 4: Explore What Happened (30 seconds)

Let's look at the pipeline that just ran:

```bash
# View the pipeline code
cat pipelines/customer_analytics.sf
```

**The pipeline** (pure SQL with SQLFlow extensions):
```sql
-- Load sample customer data
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

-- Create customer analytics
CREATE TABLE customer_summary AS
SELECT 
    c.country,
    c.tier,
    COUNT(*) as customer_count,
    AVG(c.age) as avg_age,
    COUNT(o.order_id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as total_revenue,
    ROUND(COALESCE(SUM(o.total_amount), 0) / COUNT(*), 2) as revenue_per_customer
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.country, c.tier
ORDER BY total_revenue DESC;

-- Export results
EXPORT customer_summary TO 'output/customer_summary.csv' TYPE CSV;

-- Top customers analysis
CREATE TABLE top_customers AS
SELECT 
    c.customer_id,
    c.name,
    c.country,
    c.tier,
    COUNT(o.order_id) as order_count,
    COALESCE(SUM(o.total_amount), 0) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.country, c.tier
ORDER BY total_spent DESC
LIMIT 20;

EXPORT top_customers TO 'output/top_customers.csv' TYPE CSV;
```

## 🎯 What Just Happened: The Power of SQLFlow

### **Problem**: Traditional data pipelines are slow and complex
- 15-60 minutes to set up tools
- Multiple technologies to learn
- No realistic data to test with
- Complex configuration files

### **SQLFlow Solution**: Complete workflow in pure SQL
- ✅ **90 seconds**: From installation to working analytics
- ✅ **One language**: SQL you already know for everything
- ✅ **Realistic data**: 1,000 customers, 5,000 orders automatically generated
- ✅ **No configuration**: Everything works out of the box

## 🔍 Real Business Value Created

Look at your results:

```bash
# Customer metrics by market
head output/customer_summary.csv
```

**Sample output:**
```
country,tier,customer_count,avg_age,total_orders,total_revenue,revenue_per_customer
US,Premium,45,41.2,312,89450.50,1987.79
UK,Premium,38,39.8,287,76230.25,2006.59
Germany,Standard,52,35.6,189,34560.75,664.63
```

```bash
# Your top customers
head output/top_customers.csv
```

**Sample output:**
```
customer_id,name,country,tier,order_count,total_spent
C001,Alice Johnson,US,Premium,18,4567.89
C045,Robert Chen,UK,Premium,16,4234.56
C089,Maria Garcia,Spain,Premium,15,3987.12
```

## 🚀 Next Steps: Take It Further

### Try Different Data Loading Modes

```bash
# Run the load modes example
cd ../
sqlflow init load_modes_demo
cd load_modes_demo

# See how SQLFlow handles REPLACE, APPEND, and UPSERT
sqlflow pipeline run basic_load_modes
```

### Add Python When SQL Isn't Enough

```bash
# Run the UDF examples
cd ../
sqlflow init python_udf_demo
cd python_udf_demo

# See Python functions integrated with SQL
sqlflow pipeline run customer_scoring
```

### Try Different Environments

```bash
# Run in production mode (persistent storage)
sqlflow pipeline run customer_analytics --profile production

# Run with custom variables
sqlflow pipeline run customer_analytics --vars '{"min_order_amount": 100}'
```

## 💡 What Makes This Different?

### **vs dbt**: Complete pipeline, not just transformation
- ✅ SQLFlow: Source → Transform → Export in one tool
- ❌ dbt: Transform only, requires separate ingestion/export

### **vs Airflow**: SQL-first, not Python-first  
- ✅ SQLFlow: Write pipelines in SQL
- ❌ Airflow: Write complex Python DAGs

### **vs Custom Scripts**: Built-in validation and optimization
- ✅ SQLFlow: Pre-execution validation catches errors
- ❌ Custom Scripts: Runtime failures after long processing

## 🎯 Common Use Cases You Can Solve Right Now

### **Customer Analytics** (what you just built)
```sql
-- Customer lifetime value, segmentation, churn analysis
SELECT customer_tier, AVG(total_revenue) as avg_ltv
FROM customer_summary GROUP BY customer_tier;
```

### **Sales Analytics**
```sql
-- Monthly sales trends, product performance
CREATE TABLE monthly_sales AS
SELECT 
    DATE_TRUNC('month', order_date) as month,
    SUM(total_amount) as revenue,
    COUNT(*) as order_count
FROM orders GROUP BY month ORDER BY month;
```

### **Data Quality Monitoring**
```sql
-- Null checks, duplicate detection, data freshness
CREATE TABLE data_quality AS
SELECT 
    'customers' as table_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN email IS NULL THEN 1 END) as null_emails,
    COUNT(DISTINCT customer_id) as unique_customers
FROM customers;
```

## 🔧 Troubleshooting

### "Command not found: sqlflow"
```bash
# Make sure installation worked
pip install sqlflow-core

# Check installation
pip show sqlflow-core
```

### "Permission denied"
```bash
# Use virtual environment (recommended)
python -m venv sqlflow-env
source sqlflow-env/bin/activate  # On Windows: sqlflow-env\Scripts\activate
pip install sqlflow-core
```

### "No such file or directory"
```bash
# Make sure you're in the project directory
cd customer_analytics_demo
ls -la  # Should see pipelines/, data/, output/
```

## 📚 Ready to Learn More?

### **For Data Analysts**
- [Building Analytics Pipelines](user-guides/building-analytics-pipelines.md) - Common use cases and patterns
- [Connecting Data Sources](user-guides/connecting-data-sources.md) - Load from CSV, PostgreSQL, S3

### **For Data Engineers**  
- [Technical Overview](developer-guides/technical-overview.md) - Architecture and design decisions
- [Extending SQLFlow](developer-guides/extending-sqlflow.md) - Build connectors and UDFs

### **Reference Materials**
- [CLI Commands](reference/cli-commands.md) - Complete command reference
- [SQLFlow Syntax](reference/sqlflow-syntax.md) - Language specification

## 🤝 Join the Community

- ⭐ **Star us on GitHub**: [github.com/sqlflow/sqlflow](https://github.com/sqlflow/sqlflow)
- 💬 **Ask questions**: [GitHub Discussions](https://github.com/sqlflow/sqlflow/discussions)
- 🐞 **Report issues**: [GitHub Issues](https://github.com/sqlflow/sqlflow/issues)

---

**🎉 Congratulations!** You've built working customer analytics in under 2 minutes. SQLFlow handles the pipeline complexity so you can focus on insights, not infrastructure.

**Ready for production?** SQLFlow scales from local development to enterprise deployments with the same simple SQL interface. 