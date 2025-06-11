import os
import tempfile
import time
import unittest

import pandas as pd

from sqlflow.connectors.csv.destination import CSVDestination


class TestCSVDestination(unittest.TestCase):
    """Test the CSVDestination connector."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [100, 200, 300],
            }
        )

    def test_write_success(self):
        """Test successful write to a CSV file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            file_path = f.name

        try:
            connector = CSVDestination(config={"path": file_path})
            connector.write(self.test_data, options={"index": False})

            # Verify the file was created and has the correct data
            self.assertTrue(os.path.exists(file_path))
            written_df = pd.read_csv(file_path)
            pd.testing.assert_frame_equal(self.test_data, written_df)
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    def test_write_with_options(self):
        """Test write with custom options."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            file_path = f.name

        try:
            connector = CSVDestination(config={"path": file_path})
            options = {"index": False, "sep": ";"}
            connector.write(self.test_data, options=options)

            # Verify the file was created with custom options
            self.assertTrue(os.path.exists(file_path))
            with open(file_path, "r") as f:
                content = f.read()
                self.assertIn(";", content)  # Check custom separator
                self.assertNotIn("0;", content)  # Check no index
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    def test_missing_path_config(self):
        """Test that an error is raised if 'path' is not in config."""
        with self.assertRaises(ValueError) as context:
            CSVDestination(config={})

        self.assertIn("path", str(context.exception))

    def test_write_empty_dataframe(self):
        """Test writing an empty DataFrame."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            file_path = f.name

        try:
            empty_df = pd.DataFrame(columns=["id", "name", "value"])
            connector = CSVDestination(config={"path": file_path})
            connector.write(empty_df, options={"index": False})

            # Verify the file was created
            self.assertTrue(os.path.exists(file_path))
            written_df = pd.read_csv(file_path)
            self.assertEqual(len(written_df), 0)
            self.assertEqual(list(written_df.columns), ["id", "name", "value"])
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    def test_overwrite_existing_file(self):
        """Test overwriting an existing file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write("old,data\n1,2\n")
            file_path = f.name

        try:
            connector = CSVDestination(config={"path": file_path})
            connector.write(self.test_data, options={"index": False})

            # Verify the old data was overwritten
            written_df = pd.read_csv(file_path)
            pd.testing.assert_frame_equal(self.test_data, written_df)
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    def test_config_property(self):
        """Test the config property access."""
        config = {"path": "test.csv"}
        connector = CSVDestination(config=config)
        self.assertEqual(connector.config, config)
        self.assertEqual(connector.path, "test.csv")

    def test_write_strategy_selection(self):
        """Test that appropriate write strategy is selected based on data size."""
        connector = CSVDestination(config={"path": "test.csv"})

        # Small data should use direct strategy
        small_df = pd.DataFrame({"a": [1, 2, 3]})
        self.assertEqual(connector._get_write_strategy(small_df), "direct")

        # Medium data - create data that exceeds 5MB (buffered strategy)
        rows = 1500
        cols = 100  # 1500 * 100 = 150,000 values * 50 = 7.5MB
        medium_data = {f"col_{i}": list(range(rows)) for i in range(cols)}
        medium_df = pd.DataFrame(medium_data)
        self.assertEqual(connector._get_write_strategy(medium_df), "buffered")

        # Large data - create data that exceeds 50MB (chunked strategy)
        rows = 5000
        cols = 250  # 5000 * 250 = 1,250,000 values * 50 = 62.5MB
        large_data = {f"col_{i}": list(range(rows)) for i in range(cols)}
        large_df = pd.DataFrame(large_data)
        self.assertEqual(connector._get_write_strategy(large_df), "chunked")

    def test_buffer_pool_reuse(self):
        """Test that buffer pool correctly reuses buffers."""
        # Clear buffer pool
        CSVDestination._buffer_pool.clear()

        connector = CSVDestination(config={"path": "test.csv"})

        # Get a buffer - should create new one
        buffer1 = connector._get_buffer()
        self.assertEqual(len(CSVDestination._buffer_pool), 0)

        # Return buffer - should add to pool
        connector._return_buffer(buffer1)
        self.assertEqual(len(CSVDestination._buffer_pool), 1)

        # Get buffer again - should reuse from pool
        buffer2 = connector._get_buffer()
        self.assertEqual(len(CSVDestination._buffer_pool), 0)
        self.assertIs(buffer1, buffer2)  # Same object

    def test_chunked_write_performance(self):
        """Test chunked write performance with larger dataset."""
        # Create a larger dataset to test chunked writing
        large_data = {
            "id": list(range(50000)),
            "name": [f"user_{i}" for i in range(50000)],
            "value": [i * 1.5 for i in range(50000)],
        }
        large_df = pd.DataFrame(large_data)

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            file_path = f.name

        try:
            connector = CSVDestination(config={"path": file_path})

            start_time = time.time()
            connector.write(large_df, options={"index": False})
            duration = time.time() - start_time

            # Verify file was created successfully
            self.assertTrue(os.path.exists(file_path))

            # Read back and verify data integrity (sample check)
            written_df = pd.read_csv(file_path)
            self.assertEqual(len(written_df), len(large_df))
            self.assertEqual(list(written_df.columns), list(large_df.columns))

            # Performance should be reasonable (less than 10 seconds for 50k rows)
            self.assertLess(
                duration, 10.0, f"Chunked write took too long: {duration:.2f}s"
            )

        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    def test_append_mode_chunked(self):
        """Test append mode with chunked writing."""
        # Create medium-sized data that will trigger chunked append
        data = {"id": list(range(15000)), "value": [i * 2 for i in range(15000)]}
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            file_path = f.name

        try:
            connector = CSVDestination(config={"path": file_path})

            # First write
            half_df = df.head(7500)
            connector.write(half_df, options={"index": False}, mode="replace")

            # Append the rest
            second_half = df.tail(7500)
            connector.write(second_half, options={"index": False}, mode="append")

            # Verify combined data
            written_df = pd.read_csv(file_path)
            self.assertEqual(len(written_df), len(df))

        finally:
            if os.path.exists(file_path):
                os.remove(file_path)


if __name__ == "__main__":
    unittest.main()
