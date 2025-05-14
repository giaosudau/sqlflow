"""Tests for connector base interfaces."""

from typing import Any, Dict, Iterator, List

import pyarrow as pa
import pytest

from sqlflow.connectors.base import (
    ConnectionTestResult,
    Connector,
    ConnectorState,
    ExportConnector,
    Schema,
)
from sqlflow.connectors.data_chunk import DataChunk
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


def test_connector_state_transitions():
    """Test connector state transitions."""
    connector = MockConnector()
    assert connector.state == ConnectorState.CREATED

    connector.configure({"param": "value"})
    assert connector.state == ConnectorState.CONFIGURED

    connector.validate_state(ConnectorState.CONFIGURED)

    connector.state = ConnectorState.ERROR
    with pytest.raises(ConnectorError):
        connector.validate_state(ConnectorState.READY)


def test_export_connector_state_transitions():
    """Test export connector state transitions."""
    connector = MockExportConnector()
    assert connector.state == ConnectorState.CREATED

    connector.configure({"param": "value"})
    assert connector.state == ConnectorState.CONFIGURED

    connector.validate_state(ConnectorState.CONFIGURED)

    connector.state = ConnectorState.ERROR
    with pytest.raises(ConnectorError):
        connector.validate_state(ConnectorState.READY)


def test_schema_from_dict():
    """Test Schema.from_dict method."""
    schema_dict = {
        "id": "int",
        "name": "string",
        "active": "bool",
        "score": "float",
        "created": "date",
        "updated": "timestamp",
    }

    schema = Schema.from_dict(schema_dict)

    assert schema.arrow_schema.names == [
        "id",
        "name",
        "active",
        "score",
        "created",
        "updated",
    ]
    assert schema.arrow_schema.field("id").type == pa.int64()
    assert schema.arrow_schema.field("name").type == pa.string()
    assert schema.arrow_schema.field("active").type == pa.bool_()
    assert schema.arrow_schema.field("score").type == pa.float64()
    assert schema.arrow_schema.field("created").type == pa.date32()
    assert schema.arrow_schema.field("updated").type == pa.timestamp("ns")


def test_connection_test_result():
    """Test ConnectionTestResult."""
    result = ConnectionTestResult(True)
    assert result.success
    assert result.message is None

    result = ConnectionTestResult(False, "Connection failed")
    assert not result.success
    assert result.message == "Connection failed"
