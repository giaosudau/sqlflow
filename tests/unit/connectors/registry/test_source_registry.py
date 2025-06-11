from typing import Any, Dict, Iterator, List, Optional

import pytest

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry.source_registry import (
    SourceConnectorRegistry,
    source_registry,
)


class MockSourceConnector(Connector):
    def configure(self, params: Dict[str, Any]) -> None:
        pass

    def test_connection(self) -> ConnectionTestResult:
        pass

    def discover(self) -> List[str]:
        return []

    def get_schema(self, object_name: str) -> Schema:
        import pyarrow as pa

        return Schema(pa.schema([]))

    def read(
        self,
        object_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
    ) -> Iterator[DataChunk]:
        yield from ()


def test_register_and_get_connector():
    """Test registering and retrieving a connector."""
    registry = SourceConnectorRegistry()
    registry.register("mock", MockSourceConnector)
    connector_class = registry.get("mock")
    assert connector_class == MockSourceConnector


def test_get_unknown_connector():
    """Test that getting an unknown connector raises a ValueError."""
    registry = SourceConnectorRegistry()
    with pytest.raises(ValueError, match="Unknown source connector type: unknown"):
        registry.get("unknown")


def test_singleton_instance_behavior():
    """Test the behavior of the global singleton registry."""
    # Use a unique name to avoid conflicts in the global singleton
    source_registry.register("global_mock_for_singleton_test", MockSourceConnector)
    connector_class = source_registry.get("global_mock_for_singleton_test")
    assert connector_class == MockSourceConnector
