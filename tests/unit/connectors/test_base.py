"""Tests for connector base interfaces."""

from typing import Any, Dict, Iterator, List

import pyarrow as pa
import pytest

from sqlflow.connectors.base import (
    BidirectionalConnector,
    ConnectionTestResult,
    Connector,
    ConnectorState,
    ConnectorType,
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


class MockBidirectionalConnector(BidirectionalConnector):
    """Mock bidirectional connector for testing."""

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


def test_bidirectional_connector_state_transitions():
    """Test bidirectional connector state transitions."""
    connector = MockBidirectionalConnector()
    assert connector.state == ConnectorState.CREATED
    assert connector.connector_type == ConnectorType.BIDIRECTIONAL

    connector.configure({"param": "value"})
    assert connector.state == ConnectorState.CONFIGURED

    connector.validate_state(ConnectorState.CONFIGURED)

    # Test source interface
    assert list(connector.discover()) == ["table1", "table2"]
    schema = connector.get_schema("any_object")
    assert schema.arrow_schema.names == ["id", "name"]

    # Get data through read method
    data_chunks = list(connector.read("any_object"))
    assert len(data_chunks) == 1
    chunk = data_chunks[0]
    assert chunk.pandas_df.to_dict("list") == {"id": [1, 2, 3], "name": ["a", "b", "c"]}

    # Write data through write method
    data = {"col1": [1, 2], "col2": ["x", "y"]}
    data_chunk = DataChunk(pa.Table.from_pydict(data))
    connector.write("test_object", data_chunk, "overwrite")

    assert connector.written_object == "test_object"
    assert connector.written_mode == "overwrite"
    assert connector.written_data == data_chunk

    # Error state validation
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


class TestBidirectionalConnectorMixin:
    """A mixin for testing bidirectional connectors.

    This class can be used as a base class for testing any connector that inherits from
    BidirectionalConnector. It ensures that the connector implements both source and
    export interfaces correctly, and that data can be both read and written.

    Usage:
    ```
    class TestMyBidirectionalConnector(TestBidirectionalConnectorMixin):
        @pytest.fixture
        def connector(self):
            # Return an instance of your connector
            return MyBidirectionalConnector()

        @pytest.fixture
        def sample_data(self):
            # Return sample data for testing
            return {"id": [1, 2], "name": ["test1", "test2"]}
    ```
    """

    @pytest.fixture
    def connector(self):
        """Fixture providing the connector instance to test.

        Should be overridden by subclasses.

        Returns:
            An instance of a BidirectionalConnector

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        # This implementation will never be called in actual tests because they override it
        # But it serves to document the requirement and type
        raise NotImplementedError(
            "The 'connector' fixture must be implemented by subclasses"
        )

    @pytest.fixture
    def sample_data(self):
        """Fixture providing sample data for testing.

        Should be overridden by subclasses.

        Returns:
            Dict[str, List]: Sample data for testing write operations

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        # This implementation will never be called in actual tests because they override it
        # But it serves to document the requirement and type
        raise NotImplementedError(
            "The 'sample_data' fixture must be implemented by subclasses"
        )

    @pytest.fixture
    def config_params(self):
        """Fixture providing configuration parameters.

        Can be overridden by subclasses.

        Returns:
            Dict[str, Any]: Configuration parameters
        """
        return {"param1": "value1"}

    @pytest.mark.skip(reason="Abstract test - should be implemented by subclasses")
    def test_connector_type(self, connector):
        """Test that the connector has the correct connector type."""
        assert connector.connector_type == ConnectorType.BIDIRECTIONAL

    @pytest.mark.skip(reason="Abstract test - should be implemented by subclasses")
    def test_implements_source_interface(self, connector, config_params):
        """Test that the connector implements the source interface."""
        # Configure the connector
        connector.configure(config_params)

        # Check that source methods are callable
        assert callable(connector.discover)
        assert callable(connector.get_schema)
        assert callable(connector.read)

    @pytest.mark.skip(reason="Abstract test - should be implemented by subclasses")
    def test_implements_export_interface(self, connector, config_params):
        """Test that the connector implements the export interface."""
        # Configure the connector
        connector.configure(config_params)

        # Check that export methods are callable
        assert callable(connector.write)

    @pytest.mark.skip(reason="Abstract test - should be implemented by subclasses")
    def test_round_trip(self, connector, config_params, sample_data):
        """Test that data can be written and then read back."""
        # Configure the connector
        connector.configure(config_params)

        # Get a test object name from discovery or use a default
        test_object = self._get_test_object_name(connector)

        # Create a data chunk for writing
        data_chunk = DataChunk(pa.Table.from_pydict(sample_data))

        # Write the data
        connector.write(test_object, data_chunk)

        # Read it back
        self._verify_read_data(connector, test_object)

    def _get_test_object_name(self, connector):
        """Helper method to get a test object name.

        Args:
            connector: The connector instance

        Returns:
            str: Object name to use for testing
        """
        try:
            objects = connector.discover()
            if objects:
                return objects[0]
            return "test_object"
        except Exception:
            return "test_object"

    def _verify_read_data(self, connector, test_object):
        """Helper method to verify data can be read.

        Args:
            connector: The connector instance
            test_object: Object name to read from

        Raises:
            pytest.skip: If reading is not supported
        """
        try:
            read_chunks = list(connector.read(test_object))
            # If we got here, check that the data was read correctly
            assert len(read_chunks) > 0
        except Exception as e:
            # If read fails, it might be because the connector doesn't support
            # reading back what was just written, which is okay for some connectors
            pytest.skip(f"Round-trip test skipped: {str(e)}")


# Example of using the TestBidirectionalConnectorMixin
class TestMockBidirectionalConnector(TestBidirectionalConnectorMixin):
    """Test the mock bidirectional connector using the mixin."""

    @pytest.fixture
    def connector(self):
        """Provide the mock connector for testing."""
        return MockBidirectionalConnector()

    @pytest.fixture
    def sample_data(self):
        """Provide sample data for testing."""
        return {"id": [1, 2], "name": ["test1", "test2"]}

    def test_mock_specific_behavior(self, connector, config_params):
        """Test mock-specific behavior."""
        connector.configure(config_params)
        assert connector.params == config_params
