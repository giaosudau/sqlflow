#!/usr/bin/env python3
"""
Setup script for Enhanced S3 Connector Demo test data.
Creates all required test files in MinIO bucket for the demo pipeline.
"""

import io
import json
import sys
from datetime import datetime, timedelta

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


def create_s3_client():
    """Create S3 client for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )


def ensure_bucket_exists(s3_client, bucket_name):
    """Create bucket if it doesn't exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' already exists")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"✅ Created bucket '{bucket_name}'")
            except ClientError as create_error:
                print(f"❌ Failed to create bucket: {create_error}")
                return False
        else:
            print(f"❌ Error checking bucket: {e}")
            return False
    return True


def upload_text_file(s3_client, bucket, key, content, content_type="text/plain"):
    """Upload text content to S3."""
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType=content_type,
        )
        print(f"✅ Uploaded {key}")
        return True
    except Exception as e:
        print(f"❌ Failed to upload {key}: {e}")
        return False


def upload_parquet_file(s3_client, bucket, key, df):
    """Upload DataFrame as Parquet to S3."""
    try:
        # Convert to Arrow table and then to Parquet bytes
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        print(f"✅ Uploaded {key} (Parquet)")
        return True
    except Exception as e:
        print(f"❌ Failed to upload {key}: {e}")
        return False


def create_demo_data():
    """Create main demo CSV data."""
    return """id,name,category,price,created_at
1,Laptop Pro,Electronics,1299.99,2024-01-15T10:00:00Z
2,Wireless Mouse,Electronics,29.99,2024-01-16T11:30:00Z
3,Mechanical Keyboard,Electronics,149.99,2024-01-17T09:15:00Z
4,USB-C Hub,Electronics,79.99,2024-01-18T14:20:00Z
5,Monitor Stand,Office,89.99,2024-01-19T16:45:00Z
"""


def create_partitioned_data():
    """Create partitioned test data for different years/months/days."""
    partitioned_files = {}

    # Generate data for multiple partitions
    for year in [2023, 2024]:
        for month in [1, 2, 3]:
            for day in [15, 16, 17]:
                base_date = datetime(year, month, day)

                # Create events for this partition
                events = []
                for hour in range(8, 18, 2):  # Every 2 hours from 8 AM to 6 PM
                    event_time = base_date.replace(hour=hour)
                    events.extend(
                        [
                            {
                                "event_id": f"{year}{month:02d}{day:02d}{hour:02d}001",
                                "event_timestamp": event_time.isoformat() + "Z",
                                "user_id": f"user_{(hour % 5) + 1}",
                                "action": "click",
                                "value": 1.0,
                            },
                            {
                                "event_id": f"{year}{month:02d}{day:02d}{hour:02d}002",
                                "event_timestamp": (
                                    event_time + timedelta(minutes=30)
                                ).isoformat()
                                + "Z",
                                "user_id": f"user_{(hour % 3) + 1}",
                                "action": "purchase",
                                "value": 50.0 + (hour * 10),
                            },
                        ]
                    )

                # Convert to DataFrame for Parquet
                df = pd.DataFrame(events)
                key = f"partitioned-data/year={year}/month={month:02d}/day={day:02d}/events.parquet"
                partitioned_files[key] = df

    return partitioned_files


def create_csv_data():
    """Create CSV format test data."""
    return """product_id,name,category,stock,price
1,Laptop Pro 16,Electronics,25,1599.99
2,Wireless Mouse Pro,Electronics,100,39.99
3,Mechanical Keyboard RGB,Electronics,50,179.99
4,USB-C Dock,Electronics,30,129.99
5,4K Monitor,Electronics,15,399.99
6,Webcam HD,Electronics,75,89.99
7,Headphones Wireless,Electronics,60,199.99
8,Tablet Stand,Office,40,49.99
9,Desk Lamp LED,Office,35,79.99
10,Cable Organizer,Office,80,19.99
"""


def create_json_data():
    """Create JSON/JSONL test data."""
    products = [
        {
            "product_id": 1,
            "name": "Gaming Laptop",
            "category": "Electronics",
            "price": 1899.99,
            "features": ["RGB", "High Performance", "Gaming"],
        },
        {
            "product_id": 2,
            "name": "Wireless Earbuds",
            "category": "Electronics",
            "price": 149.99,
            "features": ["Noise Cancelling", "Wireless", "Compact"],
        },
        {
            "product_id": 3,
            "name": "Smart Watch",
            "category": "Electronics",
            "price": 299.99,
            "features": ["Fitness Tracking", "Smart Notifications", "Water Resistant"],
        },
        {
            "product_id": 4,
            "name": "Bluetooth Speaker",
            "category": "Electronics",
            "price": 79.99,
            "features": ["Portable", "Wireless", "High Quality Sound"],
        },
        {
            "product_id": 5,
            "name": "Tablet Pro",
            "category": "Electronics",
            "price": 599.99,
            "features": ["Large Screen", "Stylus Support", "Professional"],
        },
    ]

    # Create JSONL format (one JSON object per line)
    jsonl_content = "\n".join(json.dumps(product) for product in products)

    # Create regular JSON array
    json_content = json.dumps(products, indent=2)

    return jsonl_content, json_content


def create_events_data():
    """Create events data for incremental loading."""
    events = []
    base_date = datetime(2024, 1, 15)

    for i in range(50):  # Create 50 events
        event_time = base_date + timedelta(hours=i * 2)
        events.append(
            {
                "event_timestamp": event_time.isoformat() + "Z",
                "event_type": ["login", "view", "purchase", "logout"][i % 4],
                "user_id": f"user_{(i % 10) + 1}",
                "session_id": f"session_{(i // 4) + 1}",
                "value": round((i * 10.5) % 100, 2),
                "metadata": f"event_data_{i}",
            }
        )

    # Convert to DataFrame for Parquet
    df = pd.DataFrame(events)
    return df


def create_legacy_data():
    """Create legacy format test data."""
    return """customer_id,name,email,signup_date,status
1,Alice Johnson,alice@example.com,2024-01-01,active
2,Bob Smith,bob@example.com,2024-01-02,active
3,Charlie Brown,charlie@example.com,2024-01-03,inactive
4,Diana Prince,diana@example.com,2024-01-04,active
5,Edward Norton,edward@example.com,2024-01-05,pending
6,Fiona Green,fiona@example.com,2024-01-06,active
7,George Wilson,george@example.com,2024-01-07,active
8,Helen Davis,helen@example.com,2024-01-08,inactive
9,Ivan Petrov,ivan@example.com,2024-01-09,active
10,Julia Roberts,julia@example.com,2024-01-10,active
"""


# flake8: noqa: C901
def main():
    """Main setup function."""
    print("🚀 Setting up Enhanced S3 Connector Demo test data...")
    print("=" * 60)

    # Create S3 client
    try:
        s3_client = create_s3_client()
        print("✅ Connected to MinIO")
    except Exception as e:
        print(f"❌ Failed to connect to MinIO: {e}")
        print("💡 Make sure MinIO is running on localhost:9000")
        return False

    bucket_name = "sqlflow-demo"

    # Ensure bucket exists
    if not ensure_bucket_exists(s3_client, bucket_name):
        return False

    success_count = 0
    total_files = 0

    print("\n📁 Creating test data files...")

    # 1. Main demo data
    print("\n1️⃣ Creating main demo data...")
    demo_data = create_demo_data()
    if upload_text_file(
        s3_client, bucket_name, "demo-data/demo_data.csv", demo_data, "text/csv"
    ):
        success_count += 1
    total_files += 1

    # 2. Partitioned data (Parquet format)
    print("\n2️⃣ Creating partitioned data...")
    partitioned_files = create_partitioned_data()
    for key, df in partitioned_files.items():
        if upload_parquet_file(s3_client, bucket_name, key, df):
            success_count += 1
        total_files += 1

    # 3. CSV data
    print("\n3️⃣ Creating CSV format data...")
    csv_data = create_csv_data()
    if upload_text_file(
        s3_client, bucket_name, "csv-data/products.csv", csv_data, "text/csv"
    ):
        success_count += 1
    total_files += 1

    # 4. JSON data
    print("\n4️⃣ Creating JSON format data...")
    jsonl_data, json_data = create_json_data()
    if upload_text_file(
        s3_client,
        bucket_name,
        "json-data/products.jsonl",
        jsonl_data,
        "application/json",
    ):
        success_count += 1
    total_files += 1

    if upload_text_file(
        s3_client, bucket_name, "json-data/products.json", json_data, "application/json"
    ):
        success_count += 1
    total_files += 1

    # 5. Events data for incremental loading
    print("\n5️⃣ Creating events data...")
    events_df = create_events_data()
    if upload_parquet_file(
        s3_client, bucket_name, "events/events_2024.parquet", events_df
    ):
        success_count += 1
    total_files += 1

    # 6. Legacy data
    print("\n6️⃣ Creating legacy format data...")
    legacy_data = create_legacy_data()
    if upload_text_file(
        s3_client, bucket_name, "legacy-data/customers.csv", legacy_data, "text/csv"
    ):
        success_count += 1
    total_files += 1

    # Summary
    print("\n" + "=" * 60)
    print("📊 Setup Summary:")
    print(f"   ✅ Successfully uploaded: {success_count}/{total_files} files")
    print(f"   🪣 Bucket: {bucket_name}")
    print("   🌐 MinIO Console: http://localhost:9001")
    print("   🔑 Credentials: minioadmin/minioadmin")

    if success_count == total_files:
        print("\n🎉 All test data created successfully!")
        print("💡 You can now run the enhanced S3 connector demo pipeline")
        return True
    else:
        print(f"\n⚠️ {total_files - success_count} files failed to upload")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
