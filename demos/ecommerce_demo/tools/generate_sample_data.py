#!/usr/bin/env python3
"""
Sample data generator for SQLFlow ecommerce demo.
This script generates sample data for testing and demonstrating SQLFlow connectors.
"""

import argparse
import csv
import datetime
import os
import random
import uuid

# Configuration
CUSTOMERS = [
    {
        "customer_id": "1234",
        "name": "John Smith",
        "email": "john.smith@example.com",
        "region": "North America",
    },
    {
        "customer_id": "3456",
        "name": "Maria Garcia",
        "email": "maria.garcia@example.com",
        "region": "Europe",
    },
    {
        "customer_id": "5678",
        "name": "Wei Zhang",
        "email": "wei.zhang@example.com",
        "region": "Asia Pacific",
    },
    {
        "customer_id": "7890",
        "name": "Ahmed Hassan",
        "email": "ahmed.hassan@example.com",
        "region": "Middle East",
    },
    {
        "customer_id": "8765",
        "name": "Emma Wilson",
        "email": "emma.wilson@example.com",
        "region": "Europe",
    },
    {
        "customer_id": "9012",
        "name": "Carlos Rodriguez",
        "email": "carlos.rodriguez@example.com",
        "region": "South America",
    },
]

PRODUCTS = [
    {
        "product_id": "PRD-123",
        "name": "Premium Headphones",
        "category": "Electronics",
        "price": 49.99,
    },
    {
        "product_id": "PRD-456",
        "name": "Ergonomic Office Chair",
        "category": "Furniture",
        "price": 129.99,
    },
    {
        "product_id": "PRD-789",
        "name": "Organic Cotton T-Shirt",
        "category": "Apparel",
        "price": 19.99,
    },
]


def generate_sales_data(date, num_records=100):
    """Generate sample sales data."""
    sales_data = []

    for _ in range(num_records):
        customer = random.choice(CUSTOMERS)
        product = random.choice(PRODUCTS)

        # Generate a random quantity between 1 and 5
        quantity = random.randint(1, 5)

        # Create order
        sales_data.append(
            {
                "order_id": str(uuid.uuid4())[:8],
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "quantity": quantity,
                "price": product["price"],
                "order_date": date,
            }
        )

    return sales_data


def save_to_csv(data, filename):
    """Save data to CSV file."""
    if not data:
        return

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    fieldnames = data[0].keys()

    with open(filename, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Generated {len(data)} records in {filename}")


def generate_data_for_date_range(
    start_date, end_date, output_dir="./data", records_per_day=100
):
    """Generate data for a range of dates."""
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        filename = os.path.join(output_dir, f"sales_{date_str}.csv")

        # Generate sales data
        sales_data = generate_sales_data(date_str, records_per_day)
        save_to_csv(sales_data, filename)

        current_date += datetime.timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample data for SQLFlow ecommerce demo"
    )
    parser.add_argument(
        "--start", type=str, help="Start date (YYYY-MM-DD)", required=True
    )
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)", required=True)
    parser.add_argument("--output", type=str, default="./data", help="Output directory")
    parser.add_argument(
        "--records", type=int, default=100, help="Number of records per day"
    )

    args = parser.parse_args()

    try:
        start_date = datetime.datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format")
        return

    generate_data_for_date_range(start_date, end_date, args.output, args.records)
    print(f"Data generation complete. Files saved in {args.output}")


if __name__ == "__main__":
    main()
