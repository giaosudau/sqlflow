import unittest

from sqlflow.connectors.new_registry.source_registry import (
    SourceRegistry,
    source_registry,
)
from tests.unit.connectors.new_base.test_source_connector import MockSourceConnector


class TestSourceRegistry(unittest.TestCase):
    def setUp(self):
        # Use a new registry for each test to ensure isolation
        self.registry = SourceRegistry()

    def test_register_and_get(self):
        """Test registering and retrieving a connector."""
        self.registry.register("mock", MockSourceConnector)
        connector_class = self.registry.get("mock")
        self.assertIs(connector_class, MockSourceConnector)

    def test_get_unknown(self):
        """Test that getting an unknown connector raises an error."""
        with self.assertRaises(ValueError):
            self.registry.get("unknown")

    def test_singleton_instance(self):
        """Test the singleton instance."""
        # This test demonstrates the behavior of the global singleton
        source_registry.register("global_mock", MockSourceConnector)
        connector_class = source_registry.get("global_mock")
        self.assertIs(connector_class, MockSourceConnector)


if __name__ == "__main__":
    unittest.main()
