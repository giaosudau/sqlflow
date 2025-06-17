"""Shared test utilities for V2 Executor tests.

This module provides common helpers that eliminate mock duplication and follow
the "minimize mocking" principle from our testing standards.

Following Kent Beck's testing principles:
- Use real objects whenever possible
- Mock only at system boundaries
- Make tests independent and fast
- Test behavior, not implementation
"""

from typing import Any, Dict, List, Optional
from unittest.mock import Mock

import pandas as pd

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry.enhanced_registry import EnhancedConnectorRegistry
from sqlflow.core.engines.duckdb.engine import DuckDBEngine
from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.observability import ObservabilityManager
from sqlflow.core.state.backends import DuckDBStateBackend
from sqlflow.core.state.watermark_manager import WatermarkManager
from sqlflow.core.variables.manager import VariableConfig, VariableManager


class LightweightConnector:
    """Real lightweight connector for testing - replaces Mock connectors."""

    __test__ = False  # Tell pytest this is not a test class

    def __init__(self, data: Optional[pd.DataFrame] = None):
        if data is not None:
            self.data = data
        else:
            self.data = pd.DataFrame({"test_col": [1, 2, 3]})
        self.written_data = None
        self.write_calls = []

    def read(self) -> List[DataChunk]:
        """Return test data as DataChunks."""
        return [DataChunk(self.data)]

    def read_chunks(self) -> List[DataChunk]:
        """Return test data as chunks."""
        return self.read()

    def write(self, data: Any, target: Optional[str] = None, **kwargs) -> None:
        """Record write operations for test verification."""
        self.written_data = data
        self.write_calls.append({"data": data, "target": target, "kwargs": kwargs})

    def test_connection(self) -> bool:
        """Always return successful connection for tests."""
        return True


class _TestConnectorRegistry(EnhancedConnectorRegistry):
    """Real connector registry that returns test connectors."""

    def __init__(self):
        super().__init__()
        self._test_connectors = {}

    def create_source_connector(
        self, connector_type: str, options: Dict[str, Any]
    ) -> LightweightConnector:
        """Return real test connector instead of mock."""
        test_data = options.get("test_data")
        if isinstance(test_data, pd.DataFrame):
            return LightweightConnector(test_data)
        return LightweightConnector()

    def create_destination_connector(
        self, target: str, options: Optional[Dict[str, Any]] = None
    ) -> LightweightConnector:
        """Return real test connector instead of mock."""
        return LightweightConnector()

    def register_test_connector(
        self, name: str, connector: LightweightConnector
    ) -> None:
        """Register a specific test connector for retrieval."""
        self._test_connectors[name] = connector

    def get_test_connector(self, name: str) -> LightweightConnector:
        """Get a registered test connector."""
        return self._test_connectors.get(name, LightweightConnector())


def create_lightweight_context(variables=None, test_data=None, config=None):
    """
    Create execution context with real implementations - no mocks!

    This helper follows our "minimize mocking" principle by using real,
    lightweight implementations instead of Mock objects.

    Args:
        variables: Optional dict of variables for testing
        test_data: Optional dict of {table_name: DataFrame} for test tables
        config: Optional configuration dict

    Returns:
        ExecutionContext with real components configured for testing
    """
    # Use ALL real implementations
    engine = DuckDBEngine(database_path=":memory:")

    # Create variable manager with optional variables
    if variables:
        variable_config = VariableConfig(cli_variables=variables)
        variable_manager = VariableManager(variable_config)
    else:
        variable_manager = VariableManager()

    # Real observability manager
    observability = ObservabilityManager(run_id="test_run")

    # Real connector registry with test connectors
    connector_registry = _TestConnectorRegistry()

    # Real state management
    state_backend = DuckDBStateBackend()  # Uses in-memory DuckDB by default
    watermark_manager = WatermarkManager(state_backend)

    context = ExecutionContext(
        sql_engine=engine,
        variable_manager=variable_manager,
        observability_manager=observability,
        connector_registry=connector_registry,
        watermark_manager=watermark_manager,
        run_id="test_run",
        config=config or {},
    )

    # Set up test data if provided
    default_test_data = {
        "customers": pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 22]}
        )
    }

    test_tables = test_data or default_test_data
    for table_name, df in test_tables.items():
        engine.register_table(table_name, df)

    return context


def create_test_step_data():
    """Create standard test data for step testing."""
    return {
        "customers": pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "age": [25, 30, 22, 35, 28],
                "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"],
            }
        ),
        "orders": pd.DataFrame(
            {
                "order_id": [101, 102, 103, 104],
                "customer_id": [1, 2, 1, 3],
                "amount": [99.99, 149.50, 75.00, 200.00],
                "status": ["completed", "pending", "completed", "shipped"],
            }
        ),
    }


def create_connector_with_data(data: pd.DataFrame) -> LightweightConnector:
    """Create a test connector with specific data."""
    return LightweightConnector(data)


def assert_no_mocks_in_context(context: ExecutionContext) -> None:
    """Assert that the context contains no Mock objects - enforce real implementations."""

    def is_mock(obj):
        return isinstance(obj, Mock) or str(type(obj)).find("Mock") != -1

    assert not is_mock(context.sql_engine), "SQL engine should be real, not mock"
    assert not is_mock(
        context.variable_manager
    ), "Variable manager should be real, not mock"
    assert not is_mock(
        context.observability_manager
    ), "Observability manager should be real, not mock"
    assert not is_mock(
        context.connector_registry
    ), "Connector registry should be real, not mock"
    assert not is_mock(
        context.watermark_manager
    ), "Watermark manager should be real, not mock"


# Backwards compatibility aliases
_create_lightweight_context = create_lightweight_context
TestConnector = LightweightConnector  # For backwards compatibility
LightweightTestConnector = LightweightConnector  # For backwards compatibility
LightweightMockConnector = LightweightConnector  # For backwards compatibility
