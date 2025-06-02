#!/usr/bin/env python3
"""Test S3 file verification for the integration demo."""

import boto3


def test_s3_verification():
    """Test S3 file verification logic."""
    try:
        client = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
        )

        # Check for the specific file
        response = client.list_objects_v2(
            Bucket="sqlflow-demo", Prefix="reports/s3_test_summary.csv"
        )
        if response.get("Contents"):
            print("SUCCESS: s3_test_summary.csv found")
            for obj in response["Contents"]:
                print(f'  File: {obj["Key"]} (size: {obj["Size"]} bytes)')
            return True
        else:
            print("ERROR: s3_test_summary.csv not found")
            # List all files in reports/ to debug
            response = client.list_objects_v2(Bucket="sqlflow-demo", Prefix="reports/")
            print("Files in reports/ directory:")
            for obj in response.get("Contents", []):
                print(f'  {obj["Key"]}')
            return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False


if __name__ == "__main__":
    success = test_s3_verification()
    exit(0 if success else 1)
