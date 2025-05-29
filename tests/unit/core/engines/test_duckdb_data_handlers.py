"""Tests for DuckDB data handlers."""

from unittest.mock import Mock

import pandas as pd
import pyarrow as pa
import pytest

from sqlflow.core.engines.duckdb.data.handlers import (
    ArrowDataHandler,
    DataHandlerFactory,
    PandasDataHandler,
)
from sqlflow.core.engines.duckdb.data.registration import DataRegistrationManager


class TestDataHandlerFactory:
    """Test cases for DataHandlerFactory."""

    def test_create_pandas_handler(self):
        """Test creation of PandasDataHandler for DataFrame."""
        data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        handler = DataHandlerFactory.create(data)

        assert isinstance(handler, PandasDataHandler)

    def test_create_arrow_handler(self):
        """Test creation of ArrowDataHandler for Arrow table."""
        data = pa.table({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        handler = DataHandlerFactory.create(data)

        assert isinstance(handler, ArrowDataHandler)

    def test_create_unsupported_type_raises_error(self):
        """Test that unsupported data types raise TypeError."""
        unsupported_data = {"col1": [1, 2, 3]}  # Dictionary

        with pytest.raises(TypeError) as exc_info:
            DataHandlerFactory.create(unsupported_data)

        assert "Unsupported data type" in str(exc_info.value)
        assert "pandas.DataFrame and pyarrow.Table" in str(exc_info.value)

    def test_create_none_raises_error(self):
        """Test that None data raises TypeError."""
        with pytest.raises(TypeError) as exc_info:
            DataHandlerFactory.create(None)

        assert "Unsupported data type" in str(exc_info.value)


class TestPandasDataHandler:
    """Test cases for PandasDataHandler."""

    def test_register(self):
        """Test pandas DataFrame registration."""
        handler = PandasDataHandler()
        mock_connection = Mock()
        data = pd.DataFrame({"col1": [1, 2, 3]})

        handler.register("test_table", data, mock_connection)

        mock_connection.register.assert_called_once_with("test_table", data)


class TestArrowDataHandler:
    """Test cases for ArrowDataHandler."""

    def test_register(self):
        """Test Arrow table registration."""
        handler = ArrowDataHandler()
        mock_connection = Mock()
        data = pa.table({"col1": [1, 2, 3]})

        handler.register("test_table", data, mock_connection)

        mock_connection.register.assert_called_once_with("test_table", data)


class TestDataRegistrationManager:
    """Test cases for DataRegistrationManager."""

    def test_register_data(self):
        """Test data registration."""
        mock_connection = Mock()
        manager = DataRegistrationManager(mock_connection)
        data = pd.DataFrame({"col1": [1, 2, 3]})

        manager.register("test_table", data)

        assert manager.is_registered("test_table")
        assert "test_table" in manager.get_registered_names()
        mock_connection.register.assert_called_once_with("test_table", data)

    def test_unregister_existing_data(self):
        """Test unregistering existing data."""
        mock_connection = Mock()
        manager = DataRegistrationManager(mock_connection)
        data = pd.DataFrame({"col1": [1, 2, 3]})

        # Register first
        manager.register("test_table", data)
        assert manager.is_registered("test_table")

        # Then unregister
        manager.unregister("test_table")

        assert not manager.is_registered("test_table")
        assert "test_table" not in manager.get_registered_names()

    def test_unregister_nonexistent_data(self):
        """Test unregistering data that doesn't exist."""
        mock_connection = Mock()
        manager = DataRegistrationManager(mock_connection)

        # Should not raise an error
        manager.unregister("nonexistent_table")

        assert not manager.is_registered("nonexistent_table")
        assert len(manager.get_registered_names()) == 0

    def test_get_registered_names_empty(self):
        """Test getting registered names when no data is registered."""
        mock_connection = Mock()
        manager = DataRegistrationManager(mock_connection)

        names = manager.get_registered_names()

        assert names == []
        assert isinstance(names, list)

    def test_get_registered_names_multiple(self):
        """Test getting registered names with multiple registered datasets."""
        mock_connection = Mock()
        manager = DataRegistrationManager(mock_connection)
        data1 = pd.DataFrame({"col1": [1, 2, 3]})
        data2 = pa.table({"col2": ["a", "b", "c"]})

        manager.register("table1", data1)
        manager.register("table2", data2)

        names = manager.get_registered_names()

        assert set(names) == {"table1", "table2"}
        assert len(names) == 2

    def test_is_registered_with_multiple_datasets(self):
        """Test is_registered with multiple datasets."""
        mock_connection = Mock()
        manager = DataRegistrationManager(mock_connection)
        data1 = pd.DataFrame({"col1": [1, 2, 3]})
        data2 = pa.table({"col2": ["a", "b", "c"]})

        manager.register("table1", data1)
        manager.register("table2", data2)

        assert manager.is_registered("table1")
        assert manager.is_registered("table2")
        assert not manager.is_registered("table3")

    def test_registration_lifecycle(self):
        """Test complete registration lifecycle."""
        mock_connection = Mock()
        manager = DataRegistrationManager(mock_connection)
        data = pd.DataFrame({"col1": [1, 2, 3]})

        # Initially empty
        assert len(manager.get_registered_names()) == 0

        # Register data
        manager.register("test_table", data)
        assert len(manager.get_registered_names()) == 1
        assert manager.is_registered("test_table")

        # Unregister data
        manager.unregister("test_table")
        assert len(manager.get_registered_names()) == 0
        assert not manager.is_registered("test_table")

    def test_register_with_unsupported_data_type(self):
        """Test that registering unsupported data type raises TypeError."""
        mock_connection = Mock()
        manager = DataRegistrationManager(mock_connection)
        unsupported_data = {"col1": [1, 2, 3]}  # Dictionary

        with pytest.raises(TypeError) as exc_info:
            manager.register("test_table", unsupported_data)

        assert "Unsupported data type" in str(exc_info.value)
        assert not manager.is_registered("test_table")
        assert len(manager.get_registered_names()) == 0

    def test_get_registered_names_order_preservation(self):
        """Test that get_registered_names preserves registration order."""
        mock_connection = Mock()
        manager = DataRegistrationManager(mock_connection)

        # Register multiple datasets in a specific order
        data1 = pd.DataFrame({"col1": [1, 2, 3]})
        data2 = pa.table({"col2": ["a", "b", "c"]})
        data3 = pd.DataFrame({"col3": [4, 5, 6]})

        manager.register("alpha", data1)
        manager.register("beta", data2)
        manager.register("gamma", data3)

        names = manager.get_registered_names()

        # Should maintain insertion order (Python 3.7+ dict behavior)
        assert names == ["alpha", "beta", "gamma"]

    def test_get_registered_names_after_partial_unregistration(self):
        """Test get_registered_names after unregistering some data."""
        mock_connection = Mock()
        manager = DataRegistrationManager(mock_connection)

        # Register multiple datasets
        data1 = pd.DataFrame({"col1": [1, 2, 3]})
        data2 = pa.table({"col2": ["a", "b", "c"]})
        data3 = pd.DataFrame({"col3": [4, 5, 6]})

        manager.register("table1", data1)
        manager.register("table2", data2)
        manager.register("table3", data3)

        # Unregister middle one
        manager.unregister("table2")

        names = manager.get_registered_names()

        # Should contain only remaining tables
        assert set(names) == {"table1", "table3"}
        assert len(names) == 2
        assert "table2" not in names

    def test_get_registered_names_immutability(self):
        """Test that modifying returned list doesn't affect internal state."""
        mock_connection = Mock()
        manager = DataRegistrationManager(mock_connection)
        data = pd.DataFrame({"col1": [1, 2, 3]})

        manager.register("test_table", data)

        # Get the list and modify it
        names = manager.get_registered_names()
        original_length = len(names)
        names.append("malicious_entry")

        # Internal state should be unchanged
        assert len(manager.get_registered_names()) == original_length
        assert "malicious_entry" not in manager.get_registered_names()
        assert manager.get_registered_names() == ["test_table"]
