import os
import tempfile
import unittest

import pandas as pd

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.parquet.source import ParquetSource


class TestParquetSource(unittest.TestCase):
    def setUp(self):
        """Set up test data and temporary files."""
        self.temp_dir = tempfile.mkdtemp()

        # Create test data
        self.test_df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "value": [10.5, 20.1, 30.7, 40.2, 50.9],
                "timestamp": pd.to_datetime(
                    [
                        "2023-01-01",
                        "2023-01-02",
                        "2023-01-03",
                        "2023-01-04",
                        "2023-01-05",
                    ]
                ),
            }
        )

        # Create single test file
        self.single_file = os.path.join(self.temp_dir, "test.parquet")
        self.test_df.to_parquet(self.single_file)

        # Create multiple test files
        self.multi_files = []
        for i in range(3):
            file_path = os.path.join(self.temp_dir, f"test_{i}.parquet")
            df_chunk = self.test_df.iloc[i : i + 2]  # 2 rows per file
            df_chunk.to_parquet(file_path)
            self.multi_files.append(file_path)

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_configure_success(self):
        """Test successful configuration."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file})

        self.assertEqual(connector.path, self.single_file)
        self.assertTrue(connector.combine_files)

    def test_configure_with_options(self):
        """Test configuration with additional options."""
        connector = ParquetSource()
        connector.configure(
            {
                "path": self.single_file,
                "columns": ["id", "name"],
                "combine_files": False,
            }
        )

        self.assertEqual(connector.path, self.single_file)
        self.assertEqual(connector.columns, ["id", "name"])
        self.assertFalse(connector.combine_files)

    def test_configure_missing_path(self):
        """Test configuration with missing path parameter."""
        connector = ParquetSource()

        with self.assertRaises(ValueError) as context:
            connector.configure({})

        self.assertIn("path", str(context.exception))

    def test_connection_success(self):
        """Test successful connection test."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file})

        result = connector.test_connection()

        self.assertIsInstance(result, ConnectionTestResult)
        self.assertTrue(result.success)
        self.assertIn("Successfully connected", result.message)

    def test_connection_file_not_found(self):
        """Test connection test with non-existent file."""
        connector = ParquetSource()
        connector.configure({"path": "/non/existent/file.parquet"})

        result = connector.test_connection()

        self.assertFalse(result.success)
        self.assertIn("No files found", result.message)

    def test_connection_not_configured(self):
        """Test connection test without configuration."""
        connector = ParquetSource()

        result = connector.test_connection()

        self.assertFalse(result.success)
        self.assertIn("not configured", result.message)

    def test_discover_single_file(self):
        """Test discovery with single file."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file})

        files = connector.discover()

        self.assertEqual(len(files), 1)
        self.assertEqual(files[0], "test.parquet")

    def test_discover_multiple_files(self):
        """Test discovery with file pattern."""
        pattern = os.path.join(self.temp_dir, "test_*.parquet")
        connector = ParquetSource()
        connector.configure({"path": pattern})

        files = connector.discover()

        self.assertEqual(len(files), 3)
        self.assertIn("test_0.parquet", files)
        self.assertIn("test_1.parquet", files)
        self.assertIn("test_2.parquet", files)

    def test_get_schema(self):
        """Test schema retrieval."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file})

        schema = connector.get_schema()

        self.assertIsNotNone(schema)
        # Check that schema contains expected fields
        schema_dict = {field.name: field.type for field in schema.arrow_schema}
        self.assertIn("id", schema_dict)
        self.assertIn("name", schema_dict)
        self.assertIn("value", schema_dict)
        self.assertIn("timestamp", schema_dict)

    def test_get_schema_with_columns(self):
        """Test schema retrieval with column filtering."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file, "columns": ["id", "name"]})

        schema = connector.get_schema()

        schema_dict = {field.name: field.type for field in schema.arrow_schema}
        self.assertEqual(len(schema_dict), 2)
        self.assertIn("id", schema_dict)
        self.assertIn("name", schema_dict)
        self.assertNotIn("value", schema_dict)

    def test_read_single_file(self):
        """Test reading data from single file."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file})

        chunks = list(connector.read())

        self.assertEqual(len(chunks), 1)
        chunk = chunks[0]
        self.assertIsInstance(chunk, DataChunk)

        df = chunk.pandas_df
        self.assertEqual(len(df), 5)
        self.assertListEqual(df["id"].tolist(), [1, 2, 3, 4, 5])

    def test_read_with_columns(self):
        """Test reading data with column selection."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file, "columns": ["id", "name"]})

        chunks = list(connector.read())

        chunk = chunks[0]
        df = chunk.pandas_df
        self.assertEqual(len(df.columns), 2)
        self.assertIn("id", df.columns)
        self.assertIn("name", df.columns)
        self.assertNotIn("value", df.columns)

    def test_read_multiple_files_combined(self):
        """Test reading multiple files with combination."""
        pattern = os.path.join(self.temp_dir, "test_*.parquet")
        connector = ParquetSource()
        connector.configure({"path": pattern, "combine_files": True})

        chunks = list(connector.read())

        # Should combine all files into chunks
        total_rows = sum(len(chunk.pandas_df) for chunk in chunks)
        self.assertEqual(total_rows, 6)  # 2 rows each from 3 files

    def test_read_multiple_files_separate(self):
        """Test reading multiple files separately."""
        pattern = os.path.join(self.temp_dir, "test_*.parquet")
        connector = ParquetSource()
        connector.configure({"path": pattern, "combine_files": False})

        chunks = list(connector.read())

        # Should have chunks from each file separately
        total_rows = sum(len(chunk.pandas_df) for chunk in chunks)
        self.assertEqual(total_rows, 6)  # 2 rows each from 3 files

    def test_read_with_batch_size(self):
        """Test reading data with custom batch size."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file})

        chunks = list(connector.read(batch_size=2))

        # Should split 5 rows into chunks of 2
        self.assertGreaterEqual(len(chunks), 2)
        total_rows = sum(len(chunk.pandas_df) for chunk in chunks)
        self.assertEqual(total_rows, 5)

    def test_supports_incremental(self):
        """Test incremental loading support."""
        connector = ParquetSource()

        self.assertTrue(connector.supports_incremental())

    def test_get_cursor_value(self):
        """Test cursor value extraction."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file})

        cursor_value = connector.get_cursor_value("test.parquet", "timestamp")

        self.assertEqual(cursor_value, pd.Timestamp("2023-01-05"))

    def test_get_cursor_value_missing_column(self):
        """Test cursor value extraction with missing column."""
        connector = ParquetSource()
        connector.configure({"path": self.single_file})

        cursor_value = connector.get_cursor_value("test.parquet", "missing_column")

        self.assertIsNone(cursor_value)

    def test_read_specific_object(self):
        """Test reading a specific file by object name."""
        pattern = os.path.join(self.temp_dir, "test_*.parquet")
        connector = ParquetSource()
        connector.configure({"path": pattern})

        chunks = list(connector.read(object_name="test_1.parquet"))

        # Should only read the specified file
        total_rows = sum(len(chunk.pandas_df) for chunk in chunks)
        self.assertEqual(total_rows, 2)

    def test_read_empty_pattern(self):
        """Test reading with pattern that matches no files."""
        pattern = os.path.join(self.temp_dir, "nonexistent_*.parquet")
        connector = ParquetSource()
        connector.configure({"path": pattern})

        chunks = list(connector.read())

        self.assertEqual(len(chunks), 0)


if __name__ == "__main__":
    unittest.main()
