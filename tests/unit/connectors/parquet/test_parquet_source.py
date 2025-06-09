import os
import unittest

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from sqlflow.connectors.parquet.source import ParquetSource


class TestParquetSource(unittest.TestCase):
    def setUp(self):
        """Set up a dummy Parquet file for testing."""
        self.file_path = "test.parquet"
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        table = pa.Table.from_pandas(df)
        pq.write_table(table, self.file_path)

    def tearDown(self):
        """Remove the dummy Parquet file."""
        os.remove(self.file_path)

    def test_read_success(self):
        """Test successful read from a Parquet file."""
        connector = ParquetSource(config={"path": self.file_path})
        df = connector.read()

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (3, 2))
        self.assertEqual(list(df.columns), ["a", "b"])

    def test_missing_path_config(self):
        """Test that an error is raised if 'path' is not in config."""
        with self.assertRaises(ValueError):
            ParquetSource(config={})


if __name__ == "__main__":
    unittest.main()
