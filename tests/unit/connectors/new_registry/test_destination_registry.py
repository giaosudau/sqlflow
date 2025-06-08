import unittest

from sqlflow.connectors.new_registry.destination_registry import (
    DestinationRegistry,
    destination_registry,
)
from tests.unit.connectors.new_base.test_destination_connector import (
    MockDestinationConnector,
)


class TestDestinationRegistry(unittest.TestCase):
    def setUp(self):
        # Use a new registry for each test to ensure isolation
        self.registry = DestinationRegistry()

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
