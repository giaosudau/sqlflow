import os
import unittest

import pandas as pd
import pyarrow.parquet as pq

from sqlflow.connectors.parquet.destination import ParquetDestination


class TestParquetDestination(unittest.TestCase):
    def setUp(self):
        """Set up a dummy DataFrame for testing."""
        self.file_path = "test_output.parquet"
        self.df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    def tearDown(self):
        """Remove the dummy Parquet file."""
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    def test_write_success(self):
        """Test successful write to a Parquet file."""
        connector = ParquetDestination(config={"path": self.file_path})
        connector.write(self.df)

        # Verify the file was created and has the correct data
        self.assertTrue(os.path.exists(self.file_path))
        written_df = pq.read_table(self.file_path).to_pandas()
        pd.testing.assert_frame_equal(self.df, written_df)

    def test_missing_path_config(self):
        """Test that an error is raised if 'path' is not in config."""
        with self.assertRaises(ValueError):
            ParquetDestination(config={})


if __name__ == "__main__":
    unittest.main()
