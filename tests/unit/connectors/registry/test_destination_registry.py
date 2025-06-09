import unittest
from typing import Dict

from sqlflow.connectors.base.destination_connector import DestinationConnector
from sqlflow.connectors.registry.destination_registry import (
    DestinationConnectorRegistry,
    destination_registry,
)


class MockDestinationConnector(DestinationConnector):
    def connect(self, config: Dict):
        pass

    def check_connection(self):
        pass

    def write(self, data, table_name):
        pass


class TestDestinationRegistry(unittest.TestCase):
    def setUp(self):
        # Use a new registry for each test to ensure isolation
        self.registry = DestinationConnectorRegistry()

    def test_register_and_get(self):
        """Test registering and retrieving a connector."""
        self.registry.register("mock", MockDestinationConnector)
        connector_class = self.registry.get("mock")
        self.assertIs(connector_class, MockDestinationConnector)

    def test_get_unknown(self):
        """Test that getting an unknown connector raises an error."""
        with self.assertRaises(ValueError):
            self.registry.get("unknown")

    def test_singleton_instance(self):
        """Test the singleton instance."""
        # This test demonstrates the behavior of the global singleton
        destination_registry.register("global_mock", MockDestinationConnector)
        connector_class = destination_registry.get("global_mock")
        self.assertIs(connector_class, MockDestinationConnector)


if __name__ == "__main__":
    unittest.main()
