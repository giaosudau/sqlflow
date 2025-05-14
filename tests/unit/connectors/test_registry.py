"""Tests for connector registry."""

from typing import Any, Dict

import pytest

from sqlflow.sqlflow.connectors import CONNECTOR_REGISTRY
from sqlflow.sqlflow.connectors.base import Connector, ExportConnector
from sqlflow.sqlflow.connectors.registry import (
    EXPORT_CONNECTOR_REGISTRY,
    get_connector_class,
    get_export_connector_class,
    register_connector,
    register_export_connector,
)


@pytest.fixture(autouse=True)
def cleanup_registry():
    """Clean up registry after each test."""
    original_connectors = CONNECTOR_REGISTRY.copy()
    original_export_connectors = EXPORT_CONNECTOR_REGISTRY.copy()

    yield

    CONNECTOR_REGISTRY.clear()
    CONNECTOR_REGISTRY.update(original_connectors)

    EXPORT_CONNECTOR_REGISTRY.clear()
    EXPORT_CONNECTOR_REGISTRY.update(original_export_connectors)


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
