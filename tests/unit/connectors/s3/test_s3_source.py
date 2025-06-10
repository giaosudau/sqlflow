import io
import unittest

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_aws

from sqlflow.connectors.s3.source import S3Source


@mock_aws
class TestS3Source(unittest.TestCase):
    def setUp(self):
        """Set up a mock S3 environment."""
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.bucket_name = "test-bucket"
        self.s3_client.create_bucket(Bucket=self.bucket_name)

        # Create dummy files with multiple rows
        self.csv_key = "test.csv"
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.csv_key,
            Body="id,value\n1,a\n2,b\n3,c",
        )

        self.parquet_key = "test.parquet"
        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        self.s3_client.put_object(
            Bucket=self.bucket_name, Key=self.parquet_key, Body=buffer.getvalue()
        )

        self.json_key = "test.jsonl"  # Use .jsonl for line-delimited JSON
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.json_key,
            Body='{"id": 1, "value": "a"}\n{"id": 2, "value": "b"}',
        )

    def test_read_csv(self):
        """Test reading a CSV file from S3."""
        connector = S3Source(config={"bucket": self.bucket_name})
        chunks = list(connector.read(object_name=self.csv_key))
        df = (
            pd.concat([chunk.to_pandas() for chunk in chunks])
            if chunks
            else pd.DataFrame()
        )
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 3)

    def test_read_parquet(self):
        """Test reading a Parquet file from S3."""
        connector = S3Source(config={"bucket": self.bucket_name})
        chunks = list(connector.read(object_name=self.parquet_key))
        df = (
            pd.concat([chunk.to_pandas() for chunk in chunks])
            if chunks
            else pd.DataFrame()
        )
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 3)

    def test_read_json(self):
        """Test reading a JSON file from S3."""
        connector = S3Source(config={"bucket": self.bucket_name})
        chunks = list(connector.read(object_name=self.json_key))
        df = (
            pd.concat([chunk.to_pandas() for chunk in chunks])
            if chunks
            else pd.DataFrame()
        )
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 2)

    def test_unsupported_format(self):
        """Test that an error is raised for unsupported formats."""
        txt_key = "test.txt"
        self.s3_client.put_object(
            Bucket=self.bucket_name, Key=txt_key, Body="some text"
        )
        connector = S3Source(config={"bucket": self.bucket_name})
        with self.assertRaises(ValueError) as context:
            list(connector.read(object_name=txt_key))
        self.assertIn("Unsupported file format", str(context.exception))

    def test_missing_uri_config(self):
        """Test that an error is raised if neither 'uri' nor 'bucket' is provided."""
        with self.assertRaises(ValueError) as context:
            S3Source(config={})
        self.assertIn("'bucket' parameter is required", str(context.exception))

    def test_invalid_uri(self):
        """Test that an error is raised if the URI is invalid."""
        with self.assertRaises(ValueError) as context:
            S3Source(config={"uri": "s3:/invalid"})
        self.assertIn("'uri' must start with 's3://'", str(context.exception))


if __name__ == "__main__":
    unittest.main()
