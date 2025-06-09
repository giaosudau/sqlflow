import os
import tempfile
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
            connector.write(self.test_data)

            # Verify the file was created and has the correct data
            self.assertTrue(os.path.exists(file_path))
            written_df = pd.read_csv(
                file_path, index_col=0
            )  # pandas adds index by default
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
            connector.write(empty_df)

            # Verify the file was created
            self.assertTrue(os.path.exists(file_path))
            written_df = pd.read_csv(file_path, index_col=0)
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


if __name__ == "__main__":
    unittest.main()
