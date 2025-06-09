import io
import unittest

import boto3
import pandas as pd
import pyarrow.parquet as pq
from moto import mock_aws

from sqlflow.connectors.s3.destination import S3Destination


@mock_aws
class TestS3Destination(unittest.TestCase):
    def setUp(self):
        """Set up a mock S3 environment."""
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.bucket_name = "test-bucket"
        self.s3_client.create_bucket(Bucket=self.bucket_name)
        self.df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    def test_write_csv(self):
        """Test writing a CSV file to S3."""
        key = "test.csv"
        uri = f"s3://{self.bucket_name}/{key}"
        connector = S3Destination(config={"uri": uri})
        connector.write(self.df)

        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        content = response["Body"].read().decode("utf-8")
        self.assertIn("a,b", content)
        self.assertIn("1,4", content)

    def test_write_parquet(self):
        """Test writing a Parquet file to S3."""
        key = "test.parquet"
        uri = f"s3://{self.bucket_name}/{key}"
        connector = S3Destination(config={"uri": uri})
        connector.write(self.df)

        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        buffer = io.BytesIO(response["Body"].read())
        table = pq.read_table(buffer)
        self.assertEqual(table.num_rows, 3)

    def test_write_json(self):
        """Test writing a JSON file to S3."""
        key = "test.json"
        uri = f"s3://{self.bucket_name}/{key}"
        connector = S3Destination(config={"uri": uri})
        connector.write(self.df)

        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        content = response["Body"].read().decode("utf-8")
        self.assertIn('{"a":1,"b":4}', content)

    def test_unsupported_format(self):
        """Test that an error is raised for unsupported formats."""
        uri = f"s3://{self.bucket_name}/test.txt"
        connector = S3Destination(config={"uri": uri})
        with self.assertRaises(ValueError):
            connector.write(self.df)

    def test_missing_uri_config(self):
        """Test that an error is raised if 'uri' is not in config."""
        with self.assertRaises(ValueError):
            S3Destination(config={})


if __name__ == "__main__":
    unittest.main()
