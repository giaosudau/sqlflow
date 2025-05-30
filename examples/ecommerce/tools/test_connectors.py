#!/usr/bin/env python3
"""Connector test script for SQLFlow ecommerce demo.
This script tests all connectors in the demo environment.
"""

import argparse
import os
import sys
import time

# Add the SQLFlow module to the path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
)

try:
    from sqlflow.connectors.postgres_connector import PostgresConnector
    from sqlflow.connectors.rest_connector import RESTExportConnector
    from sqlflow.connectors.s3_connector import S3ExportConnector
except ImportError:
    print(
        "Error: SQLFlow modules not found. Make sure SQLFlow is installed or in your Python path."
    )
    sys.exit(1)


def test_postgres_connection():
    """Test connection to PostgreSQL server."""
    print("\n🔍 Testing PostgreSQL Connector...")
    connector = PostgresConnector()

    try:
        connector.configure(
            {
                "host": "postgres",
                "port": 5432,
                "dbname": "ecommerce",
                "user": "sqlflow",
                "password": "sqlflow123",
            }
        )

        result = connector.test_connection()
        if result.success:
            print("✅ PostgreSQL connection successful!")

            # Test query execution
            print("📊 Testing query execution...")
            schema = connector.get_schema("SELECT * FROM customers LIMIT 1")
            print(f"   Schema fields: {', '.join([f.name for f in schema.fields])}")

            result_iter = connector.query(
                "SELECT COUNT(*) as customer_count FROM customers"
            )
            for chunk in result_iter:
                count = chunk.to_pandas()["customer_count"].iloc[0]
                print(f"   Found {count} customers in the database")

            return True
        else:
            print(f"❌ PostgreSQL connection failed: {result.error}")
            return False
    except Exception as e:
        print(f"❌ PostgreSQL test failed with error: {str(e)}")
        return False
    finally:
        connector.close()


def test_s3_connection():
    """Test connection to S3 (MinIO) service."""
    print("\n🔍 Testing S3 Connector...")
    connector = S3ExportConnector()

    try:
        connector.configure(
            {
                "endpoint_url": "http://minio:9000",
                "region": "us-east-1",
                "access_key": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
                "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
                "bucket": "analytics",
            }
        )

        result = connector.test_connection()
        if result.success:
            print("✅ S3 connection successful!")

            # Test bucket listing
            print("📁 Listing S3 buckets...")
            if connector.s3_client:
                response = connector.s3_client.list_buckets()
                buckets = [bucket["Name"] for bucket in response["Buckets"]]
                print(f"   Available buckets: {', '.join(buckets)}")

            return True
        else:
            print(f"❌ S3 connection failed: {result.error}")
            return False
    except Exception as e:
        print(f"❌ S3 test failed with error: {str(e)}")
        return False
    finally:
        connector.close()


def test_rest_connection():
    """Test connection to REST API (MockServer)."""
    print("\n🔍 Testing REST Connector...")
    connector = RESTExportConnector()

    try:
        connector.configure(
            {
                "base_url": "http://mockserver:1080",
                "auth_method": "bearer",
                "auth_params": {"token": os.environ.get("API_TOKEN", "demo-token")},
                "timeout": 5,
            }
        )

        result = connector.test_connection()
        if result.success:
            print("✅ REST API connection successful!")
            return True
        else:
            print(f"❌ REST API connection failed: {result.error}")
            return False
    except Exception as e:
        print(f"❌ REST API test failed with error: {str(e)}")
        return False
    finally:
        connector.close()


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Test SQLFlow connectors in the ecommerce demo environment"
    )
    parser.add_argument(
        "--postgres", action="store_true", help="Test PostgreSQL connector only"
    )
    parser.add_argument("--s3", action="store_true", help="Test S3 connector only")
    parser.add_argument("--rest", action="store_true", help="Test REST connector only")
    parser.add_argument(
        "--retries", type=int, default=3, help="Number of connection retries"
    )
    parser.add_argument(
        "--delay", type=int, default=5, help="Delay between retries in seconds"
    )
    return parser.parse_args()


def _should_test_connector(args, connector):
    return not (args.postgres or args.s3 or args.rest) or getattr(args, connector)


def _print_summary(results, args):
    print("\n📋 Connection Test Summary:")
    all_success = True
    for connector, success in results.items():
        if _should_test_connector(args, connector) and not success:
            all_success = False
            print(f"❌ {connector.upper()} connector: Failed")
        elif _should_test_connector(args, connector):
            print(f"✅ {connector.upper()} connector: Success")
    if all_success:
        print("\n✅ All tested connectors are working correctly!")
        return 0
    else:
        print(
            "\n❌ Some connector tests failed. Please check the logs above for details."
        )
        return 1


def _run_connector_tests(args):
    test_all = not (args.postgres or args.s3 or args.rest)
    results = {"postgres": False, "s3": False, "rest": False}
    for attempt in range(1, args.retries + 1):
        if attempt > 1:
            print(
                f"\n⏳ Retry attempt {attempt}/{args.retries}, waiting {args.delay} seconds..."
            )
            time.sleep(args.delay)
        if test_all or args.postgres:
            results["postgres"] = test_postgres_connection()
        if test_all or args.s3:
            results["s3"] = test_s3_connection()
        if test_all or args.rest:
            results["rest"] = test_rest_connection()
        if all(results.values()):
            break
    return results


def main():
    args = _parse_args()
    results = _run_connector_tests(args)
    return _print_summary(results, args)


if __name__ == "__main__":
    sys.exit(main())
