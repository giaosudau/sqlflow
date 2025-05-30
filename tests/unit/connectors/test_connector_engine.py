"""Tests for ConnectorEngine."""

from typing import Any, Dict, Iterator, List

import pyarrow as pa
import pytest

from sqlflow.connectors import CONNECTOR_REGISTRY
from sqlflow.connectors.base import (
    ConnectionTestResult,
    Connector,
    ConnectorState,
    ExportConnector,
    Schema,
)
from sqlflow.connectors.connector_engine import ConnectorEngine
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import EXPORT_CONNECTOR_REGISTRY
from sqlflow.core.errors import ConnectorError


class MockConnector(Connector):
    """Mock connector for testing."""

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector."""
        self.params = params
        self.state = ConnectorState.CONFIGURED

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection."""
        return ConnectionTestResult(True)

    def discover(self) -> List[str]:
        """Discover available objects."""
        return ["table1", "table2"]

    def get_schema(self, object_name: str) -> Schema:
        """Get schema for an object."""
        fields = [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ]
        return Schema(pa.schema(fields))

    def read(
        self, object_name: str, columns=None, filters=None, batch_size=10000
    ) -> Iterator[DataChunk]:
        """Read data from the source."""
        data = {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
        }
        yield DataChunk(pa.Table.from_pydict(data))


class MockFailingConnector(Connector):
    """Mock connector that fails connection test."""

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector."""
        self.params = params
        self.state = ConnectorState.CONFIGURED

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection."""
        return ConnectionTestResult(False, "Connection failed")

    def discover(self) -> List[str]:
        """Discover available objects."""
        return []

    def get_schema(self, object_name: str) -> Schema:
        """Get schema for an object."""
        fields = [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ]
        return Schema(pa.schema(fields))

    def read(
        self, object_name: str, columns=None, filters=None, batch_size=10000
    ) -> Iterator[DataChunk]:
        """Read data from the source."""
        yield from []


class MockExportConnector(ExportConnector):
    """Mock export connector for testing."""

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector."""
        self.params = params
        self.state = ConnectorState.CONFIGURED

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection."""
        return ConnectionTestResult(True)

    def write(
        self, object_name: str, data_chunk: DataChunk, mode: str = "append"
    ) -> None:
        """Write data to the destination."""
        self.written_data = data_chunk
        self.written_object = object_name
        self.written_mode = mode


@pytest.fixture(autouse=True)
def setup_mock_connectors():
    """Set up mock connectors for testing."""
    original_connectors = CONNECTOR_REGISTRY.copy()
    original_export_connectors = EXPORT_CONNECTOR_REGISTRY.copy()

    CONNECTOR_REGISTRY["MOCK"] = MockConnector
    CONNECTOR_REGISTRY["MOCK_FAILING"] = MockFailingConnector
    EXPORT_CONNECTOR_REGISTRY["MOCK_EXPORT"] = MockExportConnector

    yield

    CONNECTOR_REGISTRY.clear()
    CONNECTOR_REGISTRY.update(original_connectors)

    EXPORT_CONNECTOR_REGISTRY.clear()
    EXPORT_CONNECTOR_REGISTRY.update(original_export_connectors)


def test_connector_engine_register_connector():
    """Test registering a connector with ConnectorEngine."""
    engine = ConnectorEngine()

    engine.register_connector("test_conn", "MOCK", {"param": "value"})

    assert "test_conn" in engine.registered_connectors
    assert engine.registered_connectors["test_conn"]["type"] == "MOCK"
    assert engine.registered_connectors["test_conn"]["params"] == {"param": "value"}
    assert engine.registered_connectors["test_conn"]["instance"] is None

    with pytest.raises(ValueError):
        engine.register_connector("test_conn", "MOCK", {})

    with pytest.raises(ValueError):
        engine.register_connector("new_conn", "UNKNOWN", {})


def test_connector_engine_load_data():
    """Test loading data with ConnectorEngine."""
    engine = ConnectorEngine()

    engine.register_connector("test_conn", "MOCK", {"param": "value"})

    chunks = list(engine.load_data("test_conn", "table1"))

    assert len(chunks) == 1
    chunk = chunks[0]
    df = chunk.pandas_df
    assert df.shape == (3, 2)
    assert list(df.columns) == ["id", "name"]
    assert list(df["id"]) == [1, 2, 3]
    assert list(df["name"]) == ["a", "b", "c"]

    chunks = list(engine.load_data("test_conn", "table1", columns=["name"]))
    assert len(chunks) == 1

    with pytest.raises(ValueError):
        list(engine.load_data("unknown_conn", "table1"))

    engine.register_connector("failing_conn", "MOCK_FAILING", {})
    with pytest.raises(ConnectorError):
        list(engine.load_data("failing_conn", "table1"))


def test_connector_engine_export_data():
    """Test exporting data with ConnectorEngine."""
    engine = ConnectorEngine()

    data = {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
    }
    table = pa.Table.from_pydict(data)

    engine.export_data(table, "output_object", "MOCK_EXPORT", {"param": "value"})

    with pytest.raises(ConnectorError):
        engine.export_data(table, "output_object", "UNKNOWN", {})


def test_builtin_connectors_are_registered():
    """Test that all built-in connectors are registered after import."""
    from sqlflow.connectors import CONNECTOR_REGISTRY

    required_connectors = ["CSV", "PARQUET", "POSTGRES", "S3", "REST"]
    for connector in required_connectors:
        assert connector in CONNECTOR_REGISTRY, f"Connector {connector} not registered"
        # We don't need to check if the value is not None here since we want to ensure at least
        # a placeholder exists in the registry
