import os
import tempfile
import unittest

import pandas as pd

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import ConnectorState
from sqlflow.connectors.csv.source import CSVSource


class TestCSVSource(unittest.TestCase):
    """Test the CSVSource connector with new architecture."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_csv_content = "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n"

    def test_configure_success(self):
        """Test successful configuration of CSV source."""
        connector = CSVSource()
        params = {"path": "test.csv", "has_header": True, "delimiter": ","}

        connector.configure(params)

        self.assertEqual(connector.state, ConnectorState.CONFIGURED)
        self.assertEqual(connector.path, "test.csv")
        self.assertTrue(connector.has_header)
        self.assertEqual(connector.delimiter, ",")

    def test_configure_missing_path(self):
        """Test configuration fails when path is missing."""
        connector = CSVSource()

        with self.assertRaises(ValueError) as context:
            connector.configure({})

        self.assertIn("path", str(context.exception))

    def test_configure_with_init(self):
        """Test configuration through constructor."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            connector = CSVSource(config={"path": file_path})
            self.assertEqual(connector.state, ConnectorState.CONFIGURED)
            self.assertEqual(connector.path, file_path)
        finally:
            os.remove(file_path)

    def test_test_connection_success(self):
        """Test successful connection test."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            connector = CSVSource()
            connector.configure({"path": file_path})

            result = connector.test_connection()

            self.assertIsInstance(result, ConnectionTestResult)
            self.assertTrue(result.success)
            self.assertIn("accessible", result.message)
        finally:
            os.remove(file_path)

    def test_test_connection_file_not_found(self):
        """Test connection test with non-existent file."""
        connector = CSVSource()
        connector.configure({"path": "nonexistent.csv"})

        result = connector.test_connection()

        self.assertFalse(result.success)
        self.assertIn("not found", result.message)

    def test_discover(self):
        """Test discovery of CSV files."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            connector = CSVSource()
            connector.configure({"path": file_path})

            discovered = connector.discover()

            self.assertEqual(len(discovered), 1)
            self.assertEqual(discovered[0], file_path)
        finally:
            os.remove(file_path)

    def test_get_schema(self):
        """Test schema retrieval."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            connector = CSVSource()
            connector.configure({"path": file_path})

            schema = connector.get_schema(file_path)

            self.assertIsNotNone(schema)
            # Schema should have the expected columns
            field_names = [field.name for field in schema.arrow_schema]
            self.assertIn("id", field_names)
            self.assertIn("name", field_names)
            self.assertIn("value", field_names)
        finally:
            os.remove(file_path)

    def test_read_success(self):
        """Test successful read from a CSV file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            connector = CSVSource(config={"path": file_path})
            df = connector.read()

            self.assertIsInstance(df, pd.DataFrame)
            self.assertEqual(df.shape, (3, 3))  # 3 rows, 3 columns
            self.assertEqual(list(df.columns), ["id", "name", "value"])

            # Check data content
            self.assertEqual(df.iloc[0]["name"], "Alice")
            self.assertEqual(df.iloc[1]["name"], "Bob")
            self.assertEqual(df.iloc[2]["name"], "Charlie")
        finally:
            os.remove(file_path)

    def test_read_with_object_name(self):
        """Test read with explicit object name."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            connector = CSVSource()
            connector.configure({"path": "dummy.csv"})  # Configure with dummy path

            # Read with explicit object name
            df = connector.read(object_name=file_path)

            self.assertEqual(len(df), 3)
            self.assertEqual(list(df.columns), ["id", "name", "value"])
        finally:
            os.remove(file_path)

    def test_read_with_columns_filter(self):
        """Test reading with column filtering."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            connector = CSVSource(config={"path": file_path})
            df = connector.read(columns=["id", "name"])

            self.assertEqual(df.shape, (3, 2))  # 3 rows, 2 columns
            self.assertEqual(list(df.columns), ["id", "name"])
            self.assertNotIn("value", df.columns)
        finally:
            os.remove(file_path)

    def test_read_with_custom_options(self):
        """Test reading with custom pandas options."""
        csv_content = "id;name;value\n1;Alice;100\n2;Bob;200\n"

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(csv_content)
            file_path = f.name

        try:
            connector = CSVSource()
            connector.configure({"path": file_path, "delimiter": ";"})

            df = connector.read()

            self.assertEqual(len(df), 2)
            self.assertEqual(list(df.columns), ["id", "name", "value"])
        finally:
            os.remove(file_path)

    def test_supports_incremental(self):
        """Test incremental loading support."""
        connector = CSVSource()
        self.assertTrue(connector.supports_incremental())

    def test_backward_compatibility_config_property(self):
        """Test backward compatibility with config property."""
        connector = CSVSource()
        params = {"path": "test.csv", "has_header": True}
        connector.configure(params)

        # The config property should return the connection params
        self.assertEqual(connector.config, params)

    def test_empty_config_error(self):
        """Test that empty config dict raises error."""
        with self.assertRaises(ValueError):
            CSVSource(config={})

    def test_none_config_no_error(self):
        """Test that None config doesn't raise error."""
        connector = CSVSource(config=None)
        self.assertEqual(connector.state, ConnectorState.CREATED)

    def test_read_without_configure_fails(self):
        """Test that read fails when connector is not configured."""
        connector = CSVSource()

        with self.assertRaises(AttributeError):
            connector.read()

    def test_chunked_reading(self):
        """Test chunked reading from a CSV file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            connector = CSVSource(config={"path": file_path})
            # Use small batch size to force chunking
            chunk_iter = connector.read(batch_size=2, as_iterator=True)
            chunks = list(chunk_iter)
            self.assertEqual(len(chunks), 2)
            self.assertEqual(chunks[0].shape, (2, 3))
            self.assertEqual(chunks[1].shape, (1, 3))
        finally:
            os.remove(file_path)

    def test_column_selection_at_read_time(self):
        """Test column selection at read time (usecols)."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name

        try:
            connector = CSVSource(config={"path": file_path})
            df = connector.read(columns=["id", "value"])
            self.assertEqual(list(df.columns), ["id", "value"])
            self.assertNotIn("name", df.columns)
        finally:
            os.remove(file_path)

    def test_pyarrow_engine_reading(self):
        """Test reading CSV with PyArrow engine if available."""
        try:
            pass
        except ImportError:
            self.skipTest("pyarrow is not installed")
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(self.test_csv_content)
            file_path = f.name
        try:
            connector = CSVSource(config={"path": file_path, "engine": "pyarrow"})
            df = connector.read()
            self.assertIsInstance(df, pd.DataFrame)
            self.assertEqual(df.shape, (3, 3))
            self.assertEqual(list(df.columns), ["id", "name", "value"])
        finally:
            os.remove(file_path)


if __name__ == "__main__":
    unittest.main()
