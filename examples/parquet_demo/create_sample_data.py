#!/usr/bin/env python3
"""Create sample Parquet data for the demo."""

import os

import pandas as pd


def create_sample_data():
    """Create sample Parquet files for the demo."""
    # Create data directory
    os.makedirs("data", exist_ok=True)

    # Create sales data
    sales_data = pd.DataFrame(
        {
            "sale_id": range(1, 101),
            "customer_id": [f"CUST_{i:03d}" for i in range(1, 101)],
            "product_name": ["Product A", "Product B", "Product C"] * 33
            + ["Product A"],
            "sale_date": pd.date_range("2024-01-01", periods=100, freq="D"),
            "quantity": [1, 2, 3, 4, 5] * 20,
            "unit_price": [10.50, 25.00, 15.75, 30.25, 12.99] * 20,
        }
    )

    sales_data["total_amount"] = sales_data["quantity"] * sales_data["unit_price"]

    # Save main sales file
    sales_data.to_parquet("data/sales.parquet", index=False)

    # Create monthly files
    for month in range(1, 4):
        monthly_data = sales_data[sales_data["sale_date"].dt.month == month]
        monthly_data.to_parquet(f"data/sales_2024_{month:02d}.parquet", index=False)

    # Create customer data (fix array length mismatch)
    num_customers = 50
    customer_data = pd.DataFrame(
        {
            "customer_id": [f"CUST_{i:03d}" for i in range(1, num_customers + 1)],
            "customer_name": [f"Customer {i}" for i in range(1, num_customers + 1)],
            "email": [f"customer{i}@example.com" for i in range(1, num_customers + 1)],
            "segment": (["Premium", "Standard", "Basic"] * (num_customers // 3 + 1))[
                :num_customers
            ],
            "lifetime_value": [100.0 + i * 50 for i in range(num_customers)],
            "signup_date": pd.date_range("2023-01-01", periods=num_customers, freq="W"),
        }
    )

    customer_data.to_parquet("data/customers.parquet", index=False)

    print("✅ Sample Parquet data created successfully!")
    print("📁 Files created:")
    print("   - data/sales.parquet")
    print("   - data/sales_2024_01.parquet")
    print("   - data/sales_2024_02.parquet")
    print("   - data/sales_2024_03.parquet")
    print("   - data/customers.parquet")


if __name__ == "__main__":
    create_sample_data()
