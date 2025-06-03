#!/usr/bin/env python3

from sqlflow.connectors.s3_connector import S3Connector


def test_s3_connector():
    print("🔍 Testing S3 Connector directly")
    print("=" * 40)

    connector = S3Connector()
    params = {
        "bucket": "sqlflow-demo",
        "path_prefix": "demo-data/",
        "file_format": "csv",
        "access_key_id": "minioadmin",
        "secret_access_key": "minioadmin",
        "endpoint_url": "http://minio:9000",
        "mock_mode": False,
    }

    try:
        print("1. Configuring connector...")
        connector.configure(params)
        print("   ✅ Configuration successful")

        print("2. Testing connection...")
        result = connector.test_connection()
        print(f"   Connection test: {result.success}")
        print(f"   Message: {result.message}")

        if result.success:
            print("3. Discovering files...")
            discovered = connector.discover()
            print(f"   Discovered files: {discovered}")

            if discovered:
                print("4. Testing read operation...")
                # Try to read the first discovered file
                first_file = discovered[0]
                print(f"   Reading file: {first_file}")

                # Get schema first
                schema = connector.get_schema(first_file)
                print(f"   Schema: {schema}")

                # Read data
                for i, chunk in enumerate(connector.read(first_file)):
                    print(f"   Chunk {i}: {len(chunk.pandas_df)} rows")
                    if i == 0:  # Show first few rows
                        print(f"   Sample data: {chunk.pandas_df.head()}")
                    break

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_s3_connector()
