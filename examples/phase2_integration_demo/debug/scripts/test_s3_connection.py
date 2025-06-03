#!/usr/bin/env python3
"""Test S3/MinIO connectivity from SQLFlow container."""

import boto3


def test_s3_connection():
    """Test S3/MinIO connection and write access."""
    # Test MinIO connection with the same credentials as the profile
    client = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    try:
        print("Testing bucket access...")
        response = client.list_objects_v2(Bucket="sqlflow-demo")
        print(f'Bucket access successful. Found {response.get("KeyCount", 0)} objects.')

        # Test write access
        print("Testing write access...")
        client.put_object(
            Bucket="sqlflow-demo",
            Key="test-file.txt",
            Body=b"test content from Python script",
        )
        print("Write test successful!")

        # List files
        print("Listing all files in bucket:")
        response = client.list_objects_v2(Bucket="sqlflow-demo")
        for obj in response.get("Contents", []):
            print(f'  Found file: {obj["Key"]} (size: {obj["Size"]} bytes)')

        if not response.get("Contents"):
            print("  No files found in bucket.")

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_s3_connection()
