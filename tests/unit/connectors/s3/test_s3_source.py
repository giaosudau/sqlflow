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

        # Create dummy files
        self.csv_key = "test.csv"
        self.s3_client.put_object(
            Bucket=self.bucket_name, Key=self.csv_key, Body="a,b\n1,2"
        )

        self.parquet_key = "test.parquet"
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        self.s3_client.put_object(
            Bucket=self.bucket_name, Key=self.parquet_key, Body=buffer.getvalue()
        )

        self.json_key = "test.json"
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.json_key,
            Body='[{"a": 1, "b": 2}, {"a": 3, "b": 4}]',
        )

    def test_read_csv(self):
        """Test reading a CSV file from S3."""
        uri = f"s3://{self.bucket_name}/{self.csv_key}"
        connector = S3Source(config={"uri": uri})
        df = connector.read()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (1, 2))

    def test_read_parquet(self):
        """Test reading a Parquet file from S3."""
        uri = f"s3://{self.bucket_name}/{self.parquet_key}"
        connector = S3Source(config={"uri": uri})
        df = connector.read()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (3, 2))

    def test_read_json(self):
        """Test reading a JSON file from S3."""
        uri = f"s3://{self.bucket_name}/{self.json_key}"
        connector = S3Source(config={"uri": uri})
        df = connector.read()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (2, 2))

    def test_unsupported_format(self):
        """Test that an error is raised for unsupported formats."""
        uri = f"s3://{self.bucket_name}/test.txt"
        self.s3_client.put_object(
            Bucket=self.bucket_name, Key="test.txt", Body="some text"
        )
        connector = S3Source(config={"uri": uri})
        with self.assertRaises(ValueError):
            connector.read()

    def test_missing_uri_config(self):
        """Test that an error is raised if neither 'uri' nor 'bucket' is provided."""
        with self.assertRaises(ValueError) as context:
            S3Source(config={})
        self.assertIn("'bucket' parameter is required", str(context.exception))


if __name__ == "__main__":
    unittest.main()
