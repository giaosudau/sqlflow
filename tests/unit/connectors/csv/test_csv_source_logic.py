"""Unit tests for CSV source connector logic without file I/O operations."""

import unittest
from unittest.mock import Mock, patch

from sqlflow.connectors.base.connector import ConnectorState
from sqlflow.connectors.csv.source import CSVSource


class TestCSVSourceLogic(unittest.TestCase):
    """Test CSV source connector logic without file system dependencies."""

    def test_configuration_validation_accepts_valid_parameters(self):
        """Test that configuration accepts valid parameters."""
        connector = CSVSource()
        params = {"path": "test.csv", "has_header": True, "delimiter": ","}

        connector.configure(params)

        self.assertEqual(connector.state, ConnectorState.CONFIGURED)
        self.assertEqual(connector.path, "test.csv")
        self.assertTrue(connector.has_header)
        self.assertEqual(connector.delimiter, ",")

    def test_configuration_validation_rejects_missing_path(self):
        """Test that configuration rejects missing path parameter."""
        connector = CSVSource()

        with self.assertRaises(ValueError) as context:
            connector.configure({})

        self.assertIn("path", str(context.exception))

    def test_connection_test_validates_path_format(self):
        """Test that connection test validates path format without file access."""
        connector = CSVSource()
        connector.configure({"path": "nonexistent.csv"})

        result = connector.test_connection()

        self.assertFalse(result.success)
        self.assertIn("not found", result.message)

    def test_parameter_parsing_handles_custom_delimiters(self):
        """Test parameter parsing for custom delimiters."""
        connector = CSVSource()
        params = {"path": "test.csv", "delimiter": ";", "has_header": False}

        connector.configure(params)

        self.assertEqual(connector.delimiter, ";")
        self.assertFalse(connector.has_header)

    def test_supports_incremental_loading_returns_true(self):
        """Test that incremental loading support is correctly reported."""
        connector = CSVSource()
        self.assertTrue(connector.supports_incremental())

    def test_backward_compatibility_config_property_works(self):
        """Test backward compatibility with config property."""
        connector = CSVSource()
        params = {"path": "test.csv", "has_header": True}
        connector.configure(params)

        self.assertEqual(connector.config, params)

    def test_empty_config_raises_appropriate_error(self):
        """Test that empty config dict raises meaningful error."""
        with self.assertRaises(ValueError):
            CSVSource(config={})

    def test_none_config_leaves_connector_in_created_state(self):
        """Test that None config doesn't raise error and leaves connector unconfigured."""
        connector = CSVSource(config=None)
        self.assertEqual(connector.state, ConnectorState.CREATED)

    def test_read_without_configure_raises_attribute_error(self):
        """Test that read fails when connector is not configured."""
        connector = CSVSource()

        with self.assertRaises(AttributeError):
            connector.read()

    @patch("pandas.read_csv")
    def test_column_filtering_passes_correct_parameters(self, mock_read_csv):
        """Test that column filtering parameters are passed correctly to pandas."""
        mock_read_csv.return_value = Mock()
        connector = CSVSource()
        connector.configure({"path": "test.csv"})

        connector.read(columns=["id", "name"])

        # Verify pandas.read_csv was called with usecols parameter
        mock_read_csv.assert_called_once()
        call_args = mock_read_csv.call_args
        self.assertIn("usecols", call_args.kwargs)
        self.assertEqual(call_args.kwargs["usecols"], ["id", "name"])


if __name__ == "__main__":
    unittest.main()
