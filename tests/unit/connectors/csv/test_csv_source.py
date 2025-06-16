import unittest

import pytest

from sqlflow.connectors.base.connector import ConnectorState
from sqlflow.connectors.csv.source import CSVSource


class TestCSVSource(unittest.TestCase):
    """Test the CSVSource connector with new architecture - unit tests only."""

    def test_connector_configured_successfully_when_valid_parameters_provided(self):
        """Test successful configuration of CSV source."""
        connector = CSVSource()
        params = {"path": "test.csv", "has_header": True, "delimiter": ","}

        connector.configure(params)

        self.assertEqual(connector.state, ConnectorState.CONFIGURED)
        self.assertEqual(connector.path, "test.csv")
        self.assertTrue(connector.has_header)
        self.assertEqual(connector.delimiter, ",")

    def test_configuration_fails_when_required_path_missing(self):
        """Test configuration fails when path is missing."""
        connector = CSVSource()

        with self.assertRaises(ValueError) as context:
            connector.configure({})

        self.assertIn("path", str(context.exception))

    def test_connection_test_fails_when_file_not_found(self):
        """Test connection test with non-existent file."""
        connector = CSVSource()
        connector.configure({"path": "nonexistent.csv"})

        result = connector.test_connection()

        self.assertFalse(result.success)
        self.assertIn("not found", result.message)

    def test_incremental_loading_supported_when_queried(self):
        """Test incremental loading support."""
        connector = CSVSource()
        self.assertTrue(connector.supports_incremental())

    def test_config_property_returns_parameters_when_configured_for_backward_compatibility(
        self,
    ):
        """Test backward compatibility with config property."""
        connector = CSVSource()
        params = {"path": "test.csv", "has_header": True}
        connector.configure(params)

        # The config property should return the connection params
        self.assertEqual(connector.config, params)

    def test_configuration_fails_when_empty_config_dict_provided(self):
        """Test that empty config dict raises error."""
        with self.assertRaises(ValueError):
            CSVSource(config={})

    def test_connector_created_successfully_when_none_config_provided(self):
        """Test that None config doesn't raise error."""
        connector = CSVSource(config=None)
        self.assertEqual(connector.state, ConnectorState.CREATED)

    def test_read_operation_fails_when_connector_not_configured(self):
        """Test that read fails when connector is not configured."""
        connector = CSVSource()

        with self.assertRaises(AttributeError):
            connector.read()


@pytest.mark.usefixtures("sample_csv_config")
class TestCSVSourceWithFixtures:
    """Test CSV source using shared fixtures."""

    def test_connector_configured_with_shared_csv_config(self, sample_csv_config):
        """Test configuration using shared CSV config fixture."""
        connector = CSVSource()
        connector.configure(sample_csv_config)

        assert connector.state == ConnectorState.CONFIGURED
        assert connector.path == sample_csv_config["path"]
        assert connector.has_header == sample_csv_config["has_header"]
        assert connector.delimiter == sample_csv_config["delimiter"]

    def test_connector_supports_standard_csv_operations(self, sample_csv_config):
        """Test standard CSV operations with shared config."""
        connector = CSVSource()
        connector.configure(sample_csv_config)

        # Test incremental support
        assert connector.supports_incremental()

        # Test config property
        assert connector.config == sample_csv_config


if __name__ == "__main__":
    unittest.main()
