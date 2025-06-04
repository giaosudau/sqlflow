#!/usr/bin/env python3

from sqlflow.connectors.s3_connector import S3Connector


def test_key_mode():
    print("🔍 Testing S3 Connector with specific key")
    print("=" * 40)

    connector = S3Connector()
    params = {
        "bucket": "sqlflow-demo",
        "key": "demo-data/demo_data.csv",
        "file_format": "csv",
        "access_key_id": "minioadmin",
        "secret_access_key": "minioadmin",
        "endpoint_url": "http://minio:9000",
        "mock_mode": False,
    }

    try:
        connector.configure(params)
        result = connector.test_connection()
        print(f"Connection: {result.success}")

        if result.success:
            # Test reading with the specific key
            chunks = list(connector.read("demo-data/demo_data.csv"))
            print(f"Read success: {len(chunks)} chunks")
            if chunks:
                print(f"Rows: {len(chunks[0].pandas_df)}")
                print(f"Data preview: {chunks[0].pandas_df.head()}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_key_mode()
