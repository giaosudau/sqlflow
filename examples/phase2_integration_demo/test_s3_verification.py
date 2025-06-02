#!/usr/bin/env python3
"""Test S3 file verification for the integration demo."""

import boto3


def _create_s3_client():
    """Create S3 client by trying multiple endpoints.

    Returns:
        Tuple of (client, endpoint) or (None, None) if all fail
    """
    endpoints = [
        "http://sqlflow-minio:9000",  # Docker internal network name
        "http://minio:9000",  # Docker compose service name
        "http://localhost:9000",  # Localhost fallback
    ]

    for endpoint in endpoints:
        try:
            print(f"Trying MinIO endpoint: {endpoint}")
            client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
                region_name="us-east-1",
            )
            # Test connection
            client.list_buckets()
            print(f"SUCCESS: Connected to MinIO at {endpoint}")
            return client, endpoint
        except Exception as e:
            print(f"Failed to connect to {endpoint}: {e}")
            continue

    return None, None


def _verify_bucket_exists(client, bucket_name):
    """Verify that the specified bucket exists.

    Returns:
        bool: True if bucket exists, False otherwise
    """
    buckets = client.list_buckets()
    print(f"Available buckets: {[bucket['Name'] for bucket in buckets['Buckets']]}")

    return any(bucket["Name"] == bucket_name for bucket in buckets["Buckets"])


def _check_target_file(client, bucket_name, target_file):
    """Check if the target file exists in the bucket.

    Returns:
        bool: True if file exists, False otherwise
    """
    response = client.list_objects_v2(Bucket=bucket_name, Prefix=target_file)
    if response.get("Contents"):
        print(f"SUCCESS: {target_file} found")
        for obj in response["Contents"]:
            print(f'  File: {obj["Key"]} (size: {obj["Size"]} bytes)')
        return True
    return False


def _debug_missing_file(client, bucket_name, target_file):
    """Debug what files are available when target file is missing."""
    print(f"ERROR: {target_file} not found")
    # List all files in reports/ to debug
    print("Files in reports/ directory:")
    response = client.list_objects_v2(Bucket=bucket_name, Prefix="reports/")
    for obj in response.get("Contents", []):
        print(f'  {obj["Key"]} (size: {obj["Size"]} bytes)')

    # List all files in bucket for debugging
    print(f"All files in {bucket_name} bucket:")
    response = client.list_objects_v2(Bucket=bucket_name)
    for obj in response.get("Contents", []):
        print(f'  {obj["Key"]} (size: {obj["Size"]} bytes)')


def test_s3_verification():
    """Test S3 file verification logic."""
    try:
        # Create S3 client
        client, endpoint = _create_s3_client()
        if client is None:
            print("ERROR: Could not connect to MinIO at any endpoint")
            return False

        # Verify bucket exists
        bucket_name = "sqlflow-demo"
        if not _verify_bucket_exists(client, bucket_name):
            print(f"ERROR: Bucket '{bucket_name}' does not exist")
            return False

        # Check for the target file
        target_file = "reports/s3_test_summary.csv"
        if _check_target_file(client, bucket_name, target_file):
            return True
        else:
            _debug_missing_file(client, bucket_name, target_file)
            return False

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_s3_verification()
    exit(0 if success else 1)
