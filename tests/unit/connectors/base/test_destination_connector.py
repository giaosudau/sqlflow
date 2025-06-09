import unittest
from typing import Any, Dict, Optional

import pandas as pd

from sqlflow.connectors.base.destination_connector import DestinationConnector


class MockDestinationConnector(DestinationConnector):
    """A mock destination connector for testing."""

    def write(self, df: pd.DataFrame, options: Optional[Dict[str, Any]] = None) -> None:
        self.written_df = df


class TestDestinationConnector(unittest.TestCase):
    def test_initialization(self):
        """Test that the connector can be initialized."""
        config = {"path": "/data"}
        connector = MockDestinationConnector(config)
        self.assertEqual(connector.config, config)

    def test_write(self):
        """Test the write method."""
        connector = MockDestinationConnector({})
        df = pd.DataFrame([{"a": 1, "b": 2}])
        connector.write(df)
        self.assertTrue(hasattr(connector, "written_df"))
        self.assertTrue(df.equals(connector.written_df))


if __name__ == "__main__":
    unittest.main()
