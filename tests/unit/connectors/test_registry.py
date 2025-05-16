"""Tests for connector registry."""

from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import patch

import pyarrow as pa
import pytest

from sqlflow.connectors import BIDIRECTIONAL_CONNECTOR_REGISTRY, CONNECTOR_REGISTRY
from sqlflow.connectors.base import (
    BidirectionalConnector,
    Connector,
    ConnectorType,
    ExportConnector,
    Schema,
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import (
    EXPORT_CONNECTOR_REGISTRY,
    get_bidirectional_connector_class,
    get_connector_class,
    get_export_connector_class,
    register_bidirectional_connector,
    register_connector,
    register_export_connector,
)


@pytest.fixture(autouse=True)
def cleanup_registry():
    """Clean up registry after each test."""
    original_connectors = CONNECTOR_REGISTRY.copy()
    original_export_connectors = EXPORT_CONNECTOR_REGISTRY.copy()
    original_bidirectional_connectors = BIDIRECTIONAL_CONNECTOR_REGISTRY.copy()

    yield

    CONNECTOR_REGISTRY.clear()
    CONNECTOR_REGISTRY.update(original_connectors)

    EXPORT_CONNECTOR_REGISTRY.clear()
    EXPORT_CONNECTOR_REGISTRY.update(original_export_connectors)

    BIDIRECTIONAL_CONNECTOR_REGISTRY.clear()
    BIDIRECTIONAL_CONNECTOR_REGISTRY.update(original_bidirectional_connectors)


def test_register_connector():
    """Test register_connector decorator."""

    class MinimalConnector(Connector):
        def configure(self, params: Dict[str, Any]) -> None:
            pass

        def test_connection(self):
            pass

        def discover(self):
            pass

        def get_schema(self, object_name):
            pass

        def read(self, object_name, columns=None, filters=None, batch_size=10000):
            pass

    decorator = register_connector("TEST")
    TestConnector = decorator(MinimalConnector)

    assert "TEST" in CONNECTOR_REGISTRY
    assert CONNECTOR_REGISTRY["TEST"] == TestConnector

    with pytest.raises(ValueError):
        register_connector("TEST")(MinimalConnector)


def test_register_export_connector():
    """Test register_export_connector decorator."""

    @register_export_connector("TEST_EXPORT")
    class TestExportConnector(ExportConnector):
        def configure(self, params: Dict[str, Any]) -> None:
            pass

        def test_connection(self):
            pass

        def write(self, object_name, data_chunk, mode="append"):
            pass

    assert "TEST_EXPORT" in EXPORT_CONNECTOR_REGISTRY
    assert EXPORT_CONNECTOR_REGISTRY["TEST_EXPORT"] == TestExportConnector

    with pytest.raises(ValueError):

        @register_export_connector("TEST_EXPORT")
        class DuplicateExportConnector(ExportConnector):
            def configure(self, params: Dict[str, Any]) -> None:
                pass

            def test_connection(self):
                pass

            def write(self, object_name, data_chunk, mode="append"):
                pass


def test_register_bidirectional_connector():
    """Test register_bidirectional_connector decorator."""

    # Create and register the test connector
    test_connector_cls = _create_test_bidirectional_connector()

    # Verify registrations in the different registries
    _verify_bidirectional_registration(test_connector_cls)

    # Test duplicate registration
    _test_duplicate_bidirectional_registration()


def _create_test_bidirectional_connector():
    """Create and register a test bidirectional connector."""

    @register_bidirectional_connector("TEST_BIDIRECTIONAL")
    class TestBidirectionalConnector(BidirectionalConnector):
        def configure(self, params: Dict[str, Any]) -> None:
            pass

        def test_connection(self):
            pass

        def discover(self) -> List[str]:
            return []

        def get_schema(self, object_name: str) -> Schema:
            return Schema(pa.schema([]))

        def read(
            self,
            object_name: str,
            columns: Optional[List[str]] = None,
            filters: Optional[Dict[str, Any]] = None,
            batch_size: int = 10000,
        ) -> Iterator[DataChunk]:
            yield DataChunk(pa.table({}))

        def write(
            self, object_name: str, data_chunk: DataChunk, mode: str = "append"
        ) -> None:
            pass

    return TestBidirectionalConnector


def _verify_bidirectional_registration(cls):
    """Verify that the class is properly registered in all registries."""
    # Verify registrations
    assert "TEST_BIDIRECTIONAL" in BIDIRECTIONAL_CONNECTOR_REGISTRY
    assert BIDIRECTIONAL_CONNECTOR_REGISTRY["TEST_BIDIRECTIONAL"] == cls

    # Check that it's also registered in the source and export registries
    assert "TEST_BIDIRECTIONAL" in CONNECTOR_REGISTRY
    assert "TEST_BIDIRECTIONAL" in EXPORT_CONNECTOR_REGISTRY

    # Verify its connector type is set correctly
    connector = cls()
    assert connector.connector_type == ConnectorType.BIDIRECTIONAL


def _test_duplicate_bidirectional_registration():
    """Test that registering a duplicate connector raises an error."""
    with pytest.raises(ValueError, match="already registered"):

        @register_bidirectional_connector("TEST_BIDIRECTIONAL")
        class DuplicateConnector(BidirectionalConnector):
            def configure(self, params: Dict[str, Any]) -> None:
                pass

            def test_connection(self):
                pass

            def discover(self) -> List[str]:
                return []

            def get_schema(self, object_name: str) -> Schema:
                return Schema(pa.schema([]))

            def read(
                self,
                object_name: str,
                columns: Optional[List[str]] = None,
                filters: Optional[Dict[str, Any]] = None,
                batch_size: int = 10000,
            ) -> Iterator[DataChunk]:
                yield DataChunk(pa.table({}))

            def write(
                self, object_name: str, data_chunk: DataChunk, mode: str = "append"
            ) -> None:
                pass


def test_bidirectional_connector_validation():
    """Test validation of bidirectional connector implementations."""

    # Helper class to test validation
    class TestConnectorBase:
        """Base connector for testing validation."""

        def __init__(self):
            self.connector_type = ConnectorType.BIDIRECTIONAL

        def configure(self, params: Dict[str, Any]) -> None:
            pass

        def test_connection(self):
            pass

        def discover(self) -> List[str]:
            return []

        def get_schema(self, object_name: str) -> Schema:
            return Schema(pa.schema([]))

    # Create a connector missing the read method
    class MissingReadConnector(TestConnectorBase):
        # Has all required methods except read

        def write(
            self, object_name: str, data_chunk: DataChunk, mode: str = "append"
        ) -> None:
            pass

    # Create a connector missing the write method
    class MissingWriteConnector(TestConnectorBase):
        # Has all required methods except write

        def read(
            self,
            object_name: str,
            columns: Optional[List[str]] = None,
            filters: Optional[Dict[str, Any]] = None,
            batch_size: int = 10000,
        ) -> Iterator[DataChunk]:
            yield DataChunk(pa.table({}))

    # Mock inspect.getmembers to return our controlled set of methods
    with patch("inspect.getmembers") as mock_getmembers:
        # For MissingReadConnector, return all methods except read
        mock_getmembers.return_value = [
            ("configure", MissingReadConnector.configure),
            ("test_connection", MissingReadConnector.test_connection),
            ("discover", MissingReadConnector.discover),
            ("get_schema", MissingReadConnector.get_schema),
            ("write", MissingReadConnector.write),
            # read is missing
        ]

        # Test validation when missing read method
        with pytest.raises(ValueError, match="must implement read\\(\\) method"):
            register_bidirectional_connector("MISSING_READ")(MissingReadConnector)

        # For MissingWriteConnector, return all methods except write
        mock_getmembers.return_value = [
            ("configure", MissingWriteConnector.configure),
            ("test_connection", MissingWriteConnector.test_connection),
            ("discover", MissingWriteConnector.discover),
            ("get_schema", MissingWriteConnector.get_schema),
            ("read", MissingWriteConnector.read),
            # write is missing
        ]

        # Test validation when missing write method
        with pytest.raises(ValueError, match="must implement write\\(\\) method"):
            register_bidirectional_connector("MISSING_WRITE")(MissingWriteConnector)


def test_get_connector_class():
    """Test get_connector_class function."""

    @register_connector("TEST_GET")
    class TestConnector(Connector):
        def configure(self, params: Dict[str, Any]) -> None:
            pass

        def test_connection(self):
            pass

        def discover(self):
            pass

        def get_schema(self, object_name):
            pass

        def read(self, object_name, columns=None, filters=None, batch_size=10000):
            pass

    cls = get_connector_class("TEST_GET")
    assert cls == TestConnector

    with pytest.raises(ValueError):
        get_connector_class("UNKNOWN")


def test_get_export_connector_class():
    """Test get_export_connector_class function."""

    @register_export_connector("TEST_EXPORT_GET")
    class TestExportConnector(ExportConnector):
        def configure(self, params: Dict[str, Any]) -> None:
            pass

        def test_connection(self):
            pass

        def write(self, object_name, data_chunk, mode="append"):
            pass

    cls = get_export_connector_class("TEST_EXPORT_GET")
    assert cls == TestExportConnector

    with pytest.raises(ValueError):
        get_export_connector_class("UNKNOWN")


def test_get_bidirectional_connector_class():
    """Test get_bidirectional_connector_class function."""

    @register_bidirectional_connector("TEST_BIDIR_GET")
    class TestBidirConnector(BidirectionalConnector):
        def configure(self, params: Dict[str, Any]) -> None:
            pass

        def test_connection(self):
            pass

        def discover(self) -> List[str]:
            return []

        def get_schema(self, object_name: str) -> Schema:
            return Schema(pa.schema([]))

        def read(
            self,
            object_name: str,
            columns: Optional[List[str]] = None,
            filters: Optional[Dict[str, Any]] = None,
            batch_size: int = 10000,
        ) -> Iterator[DataChunk]:
            yield DataChunk(pa.table({}))

        def write(
            self, object_name: str, data_chunk: DataChunk, mode: str = "append"
        ) -> None:
            pass

    cls = get_bidirectional_connector_class("TEST_BIDIR_GET")
    assert cls == TestBidirConnector

    with pytest.raises(ValueError):
        get_bidirectional_connector_class("UNKNOWN")
