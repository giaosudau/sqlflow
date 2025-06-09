import unittest
from typing import Any, Dict, Optional

import pandas as pd

from sqlflow.connectors.base.source_connector import SourceConnector


class MockSourceConnector(SourceConnector):
    """A mock source connector for testing."""

    def read(self, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        return pd.DataFrame([{"a": 1, "b": 2}])


class TestSourceConnector(unittest.TestCase):
    def test_initialization(self):
        """Test that the connector can be initialized."""
        config = {"path": "/data"}
        connector = MockSourceConnector(config)
        self.assertEqual(connector.config, config)

    def test_read(self):
        """Test the read method."""
        connector = MockSourceConnector({})
        df = connector.read()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)


if __name__ == "__main__":
    unittest.main()
