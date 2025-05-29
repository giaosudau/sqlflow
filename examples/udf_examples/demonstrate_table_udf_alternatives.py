#!/usr/bin/env python3
"""Demonstration of table UDF alternatives using external processing.

This script shows how to achieve table UDF functionality by:
1. Fetching data from DuckDB
2. Processing it with pandas outside of SQL
3. Registering the results back with DuckDB

This approach works around DuckDB's table function limitations.
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from examples.udf_examples.python_udfs.table_udf_alternatives import (
    add_sales_metrics_external,
    detect_outliers_external,
)
from sqlflow.core.engines.duckdb import DuckDBEngine


def demonstrate_external_processing():
    """Demonstrate external processing approach to table UDFs."""
    print("🔧 Table UDF Alternatives: External Processing Demo")
    print("=" * 60)

    # Initialize DuckDB engine
    engine = DuckDBEngine(":memory:")

    if not engine.connection:
        raise RuntimeError("Failed to establish DuckDB connection")

    # Load sample data
    print("\n📥 Loading sample data...")

    # Load customers
    customers_path = Path(__file__).parent / "data" / "customers.csv"
    customers_df = pd.read_csv(customers_path)
    engine.connection.register("customers", customers_df)
    print(f"   Loaded customers: {len(customers_df)} rows")

    # Load sales
    sales_path = Path(__file__).parent / "data" / "sales.csv"
    sales_df = pd.read_csv(sales_path)
    engine.connection.register("sales", sales_df)
    print(f"   Loaded sales: {len(sales_df)} rows")

    # ===== APPROACH 1: EXTERNAL PROCESSING =====
    print("\n🔄 Approach 1: External Processing with Pandas")
    print("-" * 50)

    # Step 1: Fetch data from DuckDB
    print("   Step 1: Fetching data from DuckDB...")
    sales_result = engine.execute_query("SELECT * FROM sales")
    sales_for_processing = sales_result.fetchdf()
    print(f"   Retrieved {len(sales_for_processing)} sales records")

    # Step 2: Process data externally with pandas
    print("   Step 2: Processing data externally...")

    # Apply sales metrics transformation
    sales_with_metrics = add_sales_metrics_external(sales_for_processing)
    print(f"   Added sales metrics: {list(sales_with_metrics.columns)}")

    # Apply outlier detection
    sales_with_outliers = detect_outliers_external(sales_with_metrics, "price")
    print(
        f"   Added outlier detection: total columns = {len(sales_with_outliers.columns)}"
    )

    # Step 3: Register processed data back with DuckDB
    print("   Step 3: Registering processed data with DuckDB...")
    engine.connection.register("sales_processed_external", sales_with_outliers)

    # Step 4: Query the processed data with SQL
    print("   Step 4: Querying processed data...")

    result = engine.execute_query(
        """
        SELECT 
            id,
            product,
            price,
            quantity,
            total,
            tax,
            final_price,
            z_score,
            is_outlier,
            percentile
        FROM sales_processed_external
        ORDER BY price DESC
        LIMIT 5
    """
    )

    processed_data = result.fetchdf()
    print("\n   📊 Top 5 sales by price (with calculated metrics):")
    print(processed_data.to_string(index=False))

    # ===== APPROACH 2: HYBRID PROCESSING =====
    print("\n\n🔄 Approach 2: Hybrid SQL + External Processing")
    print("-" * 50)

    # Step 1: Use SQL for initial aggregation
    print("   Step 1: SQL aggregation...")
    customer_sales_result = engine.execute_query(
        """
        SELECT 
            c.id,
            c.name,
            c.email,
            COUNT(s.id) as num_purchases,
            SUM(s.price * s.quantity) as total_spent,
            AVG(s.price) as avg_price,
            MIN(s.date) as first_purchase,
            MAX(s.date) as last_purchase
        FROM customers c
        LEFT JOIN sales s ON c.id = s.customer_id
        GROUP BY c.id, c.name, c.email
        ORDER BY total_spent DESC NULLS LAST
    """
    )

    customer_agg = customer_sales_result.fetchdf()
    print(f"   Aggregated customer data: {len(customer_agg)} customers")

    # Step 2: External processing for complex calculations
    print("   Step 2: External processing for customer segmentation...")

    # Add customer segmentation based on spending
    def segment_customers(df):
        """Segment customers based on spending patterns."""
        df = df.copy()

        # Handle null values for customers with no purchases
        df["total_spent"] = df["total_spent"].fillna(0)
        df["num_purchases"] = df["num_purchases"].fillna(0)

        # Calculate spending percentiles
        spending_95th = df["total_spent"].quantile(0.95)
        spending_75th = df["total_spent"].quantile(0.75)
        spending_25th = df["total_spent"].quantile(0.25)

        # Segment customers
        conditions = [
            df["total_spent"] >= spending_95th,
            df["total_spent"] >= spending_75th,
            df["total_spent"] >= spending_25th,
            df["total_spent"] > 0,
            df["total_spent"] == 0,
        ]

        choices = ["VIP", "High Value", "Regular", "Low Value", "Inactive"]
        df["customer_segment"] = np.select(conditions, choices, default="Unknown")

        # Calculate customer lifetime value score
        df["clv_score"] = (
            df["total_spent"] * 0.6 + df["num_purchases"] * 10 * 0.4
        ).round(2)

        return df

    customer_segmented = segment_customers(customer_agg)

    # Step 3: Register segmented data
    print("   Step 3: Registering segmented customer data...")
    engine.connection.register("customers_segmented", customer_segmented)

    # Step 4: Final analysis with SQL
    print("   Step 4: Final analysis with SQL...")

    segment_analysis = engine.execute_query(
        """
        SELECT 
            customer_segment,
            COUNT(*) as customer_count,
            AVG(total_spent) as avg_spending,
            AVG(num_purchases) as avg_purchases,
            AVG(clv_score) as avg_clv_score
        FROM customers_segmented
        GROUP BY customer_segment
        ORDER BY avg_spending DESC
    """
    )

    segment_data = segment_analysis.fetchdf()
    print("\n   📊 Customer Segmentation Analysis:")
    print(segment_data.to_string(index=False))

    # ===== APPROACH 3: REAL-TIME PROCESSING =====
    print("\n\n🔄 Approach 3: Real-time External Processing")
    print("-" * 50)

    print("   Simulating real-time data processing...")

    # Simulate new sales data
    new_sales = pd.DataFrame(
        {
            "id": [11, 12, 13],
            "customer_id": [1, 2, 3],
            "product": ["New Product A", "New Product B", "New Product C"],
            "price": [99.99, 149.99, 199.99],
            "quantity": [2, 1, 3],
            "date": ["2024-01-15", "2024-01-16", "2024-01-17"],
        }
    )

    print(f"   Processing {len(new_sales)} new sales records...")

    # Process new data
    new_sales_processed = add_sales_metrics_external(new_sales)
    new_sales_with_outliers = detect_outliers_external(new_sales_processed, "price")

    # Register new data
    engine.connection.register("new_sales_processed", new_sales_with_outliers)

    # Combine with existing data for analysis
    combined_result = engine.execute_query(
        """
        SELECT 
            'existing' as data_source,
            COUNT(*) as record_count,
            AVG(total) as avg_total,
            SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) as outlier_count
        FROM sales_processed_external
        
        UNION ALL
        
        SELECT 
            'new' as data_source,
            COUNT(*) as record_count,
            AVG(total) as avg_total,
            SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) as outlier_count
        FROM new_sales_processed
        
        ORDER BY data_source
    """
    )

    combined_data = combined_result.fetchdf()
    print("\n   📊 Existing vs New Data Comparison:")
    print(combined_data.to_string(index=False))

    print("\n" + "=" * 60)
    print("✅ External Processing Demo Complete!")
    print("\nKey Benefits of External Processing Approach:")
    print("• ✅ Full pandas functionality for complex transformations")
    print("• ✅ No DuckDB table function limitations")
    print("• ✅ Can leverage any Python library (scikit-learn, numpy, etc.)")
    print("• ✅ Easy to test and debug outside of SQL")
    print("• ✅ Can process data in batches or real-time")
    print("• ✅ Results seamlessly integrate back into DuckDB")

    print("\nWhen to use this approach:")
    print("• Complex data transformations requiring pandas/numpy")
    print("• Machine learning preprocessing")
    print("• Data that needs external API calls")
    print("• When table UDF functionality is needed but not supported")


if __name__ == "__main__":
    # Change to the examples directory
    os.chdir(Path(__file__).parent)

    try:
        demonstrate_external_processing()
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
