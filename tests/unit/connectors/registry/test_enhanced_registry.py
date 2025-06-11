"""Unit tests for the enhanced connector registry."""

from typing import Any, Dict, Iterator, List, Optional

import pytest

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector
from sqlflow.connectors.base.destination_connector import DestinationConnector
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry.enhanced_registry import (
    ConfigurationResolutionResult,
    ConnectorTypeInfo,
    EnhancedConnectorRegistry,
    enhanced_registry,
)


# Mock connector classes for testing
class MockSourceConnector(Connector):
    """Mock source connector for testing."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__()
        if config is not None:
            self.configure(config)

    def configure(self, params: Dict[str, Any]) -> None:
        self.connection_params = params

    def test_connection(self) -> ConnectionTestResult:
        return ConnectionTestResult(success=True)

    def discover(self) -> List[str]:
        return ["mock_object"]

    def get_schema(self, object_name: str) -> Schema:
        import pyarrow as pa

        return Schema(pa.schema([]))

    def read(self, object_name: str, **kwargs) -> Iterator[DataChunk]:
        yield DataChunk(data=[])


class MockDestinationConnector(DestinationConnector):
    """Mock destination connector for testing."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    def write(self, df, options: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        """Mock write method."""


class MockLegacyConnector(Connector):
    """Mock legacy connector that doesn't use config parameter in init."""

    def __init__(self):
        super().__init__()
        self.connection_params = {}

    def configure(self, params: Dict[str, Any]) -> None:
        self.connection_params = params

    def test_connection(self) -> ConnectionTestResult:
        return ConnectionTestResult(success=True)

    def discover(self) -> List[str]:
        return ["mock_legacy_object"]

    def get_schema(self, object_name: str) -> Schema:
        import pyarrow as pa

        return Schema(pa.schema([]))

    def read(self, object_name: str, **kwargs) -> Iterator[DataChunk]:
        yield DataChunk(data=[])


class TestEnhancedConnectorRegistry:
    """Test cases for EnhancedConnectorRegistry."""

    @pytest.fixture
    def registry(self):
        """Create a fresh registry for each test."""
        return EnhancedConnectorRegistry()

    def test_register_source_connector_basic(self, registry):
        """Test basic source connector registration."""
        registry.register_source("mock", MockSourceConnector)

        # Verify registration
        connector_class = registry.get_source_connector_class("mock")
        assert connector_class == MockSourceConnector

        # Verify it's in the list
        assert "mock" in registry.list_source_connectors()

    def test_register_source_connector_with_metadata(self, registry):
        """Test source connector registration with full metadata."""
        default_config = {"timeout": 30, "retries": 3}
        required_params = ["host", "port"]
        optional_params = {"ssl": False}
        description = "Mock connector for testing"

        registry.register_source(
            "mock",
            MockSourceConnector,
            default_config=default_config,
            required_params=required_params,
            optional_params=optional_params,
            description=description,
        )

        # Verify connector info
        info = registry.get_source_connector_info("mock")
        assert info.connector_type == "mock"
        assert info.default_config == default_config
        assert info.required_params == required_params
        assert info.optional_params == optional_params
        assert info.description == description

    def test_register_destination_connector_basic(self, registry):
        """Test basic destination connector registration."""
        registry.register_destination("mock_dest", MockDestinationConnector)

        # Verify registration
        connector_class = registry.get_destination_connector_class("mock_dest")
        assert connector_class == MockDestinationConnector

        # Verify it's in the list
        assert "mock_dest" in registry.list_destination_connectors()

    def test_get_unknown_connector_raises_error(self, registry):
        """Test that getting unknown connectors raises appropriate errors."""
        with pytest.raises(ValueError, match="Unknown source connector type"):
            registry.get_source_connector_class("unknown")

        with pytest.raises(ValueError, match="Unknown destination connector type"):
            registry.get_destination_connector_class("unknown")

    def test_configuration_resolution_defaults_only(self, registry):
        """Test configuration resolution with only defaults."""
        default_config = {"timeout": 30, "retries": 3}
        registry.register_source(
            "mock", MockSourceConnector, default_config=default_config
        )

        result = registry.resolve_configuration("mock", is_source=True)

        assert result.resolved_config == default_config
        assert result.profile_used is False
        assert result.overridden_params == []

    def test_configuration_resolution_with_profile_params(self, registry):
        """Test configuration resolution with profile parameters."""
        default_config = {"timeout": 30, "retries": 3}
        profile_params = {"timeout": 60, "host": "localhost"}

        registry.register_source(
            "mock", MockSourceConnector, default_config=default_config
        )

        result = registry.resolve_configuration(
            "mock", is_source=True, profile_params=profile_params
        )

        expected_config = {"timeout": 60, "retries": 3, "host": "localhost"}
        assert result.resolved_config == expected_config
        assert result.profile_used is True
        assert result.overridden_params == []

    def test_configuration_resolution_with_overrides(self, registry):
        """Test configuration resolution with override options."""
        default_config = {"timeout": 30, "retries": 3}
        profile_params = {"timeout": 60, "host": "localhost"}
        override_options = {"timeout": 120, "ssl": True}

        registry.register_source(
            "mock", MockSourceConnector, default_config=default_config
        )

        result = registry.resolve_configuration(
            "mock",
            is_source=True,
            profile_params=profile_params,
            override_options=override_options,
        )

        expected_config = {
            "timeout": 120,  # Override wins
            "retries": 3,  # Default
            "host": "localhost",  # Profile
            "ssl": True,  # Override
        }
        assert result.resolved_config == expected_config
        assert result.profile_used is True
        assert "timeout" in result.overridden_params

    def test_configuration_validation_warnings(self, registry):
        """Test configuration validation with missing required params."""
        registry.register_source(
            "mock", MockSourceConnector, required_params=["host", "port"]
        )

        result = registry.resolve_configuration(
            "mock", is_source=True, profile_params={"host": "localhost"}  # Missing port
        )

        assert len(result.validation_warnings) > 0
        assert any("port" in warning for warning in result.validation_warnings)

    def test_create_source_connector_with_config(self, registry):
        """Test creating source connector with config parameter."""
        registry.register_source("mock", MockSourceConnector)

        config = {"host": "localhost", "port": 5432}
        connector = registry.create_source_connector("mock", config)

        assert isinstance(connector, MockSourceConnector)
        # The real check is that configure was called
        assert connector.connection_params == config

    def test_create_source_connector_legacy_fallback(self, registry):
        """Test creating legacy connector that doesn't use config parameter."""
        registry.register_source("mock_legacy", MockLegacyConnector)

        config = {"host": "localhost", "port": 5432}
        connector = registry.create_source_connector("mock_legacy", config)

        assert isinstance(connector, MockLegacyConnector)
        assert connector.connection_params == config

    def test_create_destination_connector(self, registry):
        """Test creating destination connector."""
        registry.register_destination("mock_dest", MockDestinationConnector)

        config = {"path": "/tmp/output.csv"}
        connector = registry.create_destination_connector("mock_dest", config)

        assert isinstance(connector, MockDestinationConnector)
        assert connector.config == config

    def test_get_connector_defaults(self, registry):
        """Test getting connector defaults."""
        default_config = {"timeout": 30, "retries": 3}
        registry.register_source(
            "mock", MockSourceConnector, default_config=default_config
        )

        defaults = registry.get_connector_defaults("mock", is_source=True)
        assert defaults == default_config

        # Verify it's a copy (not the same object)
        defaults["timeout"] = 60
        original_defaults = registry.get_connector_defaults("mock", is_source=True)
        assert original_defaults["timeout"] == 30

    def test_list_connectors(self, registry):
        """Test listing registered connectors."""
        registry.register_source("mock1", MockSourceConnector)
        registry.register_source("mock2", MockSourceConnector)
        registry.register_destination("dest1", MockDestinationConnector)

        sources = registry.list_source_connectors()
        destinations = registry.list_destination_connectors()

        assert "mock1" in sources
        assert "mock2" in sources
        assert "dest1" in destinations
        assert len(sources) == 2
        assert len(destinations) == 1


class TestConnectorTypeInfo:
    """Test cases for ConnectorTypeInfo dataclass."""

    def test_connector_type_info_creation(self):
        """Test creating ConnectorTypeInfo with all fields."""
        info = ConnectorTypeInfo(
            connector_class=MockSourceConnector,
            connector_type="test",
            default_config={"timeout": 30},
            required_params=["host"],
            optional_params={"ssl": False},
            param_types={"timeout": int},
            description="Test connector",
        )

        assert info.connector_class == MockSourceConnector
        assert info.connector_type == "test"
        assert info.default_config == {"timeout": 30}
        assert info.required_params == ["host"]
        assert info.optional_params == {"ssl": False}
        assert info.param_types == {"timeout": int}
        assert info.description == "Test connector"

    def test_connector_type_info_defaults(self):
        """Test ConnectorTypeInfo with default values."""
        info = ConnectorTypeInfo(
            connector_class=MockSourceConnector, connector_type="test"
        )

        assert info.default_config == {}
        assert info.required_params == []
        assert info.optional_params == {}
        assert info.param_types == {}
        assert info.description == ""


class TestConfigurationResolutionResult:
    """Test cases for ConfigurationResolutionResult dataclass."""

    def test_configuration_resolution_result_creation(self):
        """Test creating ConfigurationResolutionResult."""
        result = ConfigurationResolutionResult(
            resolved_config={"host": "localhost"},
            profile_used="test_profile",
            overridden_params=["timeout"],
            validation_warnings=["Missing port"],
        )

        assert result.resolved_config == {"host": "localhost"}
        assert result.profile_used == "test_profile"
        assert result.overridden_params == ["timeout"]
        assert result.validation_warnings == ["Missing port"]

    def test_configuration_resolution_result_defaults(self):
        """Test ConfigurationResolutionResult with default values."""
        result = ConfigurationResolutionResult(resolved_config={"host": "localhost"})

        assert result.profile_used is None
        assert result.overridden_params == []
        assert result.validation_warnings == []


class TestGlobalEnhancedRegistry:
    """Test cases for the global enhanced registry instance."""

    def test_global_registry_exists(self):
        """Test that global enhanced registry exists."""
        assert enhanced_registry is not None
        assert isinstance(enhanced_registry, EnhancedConnectorRegistry)

    def test_global_registry_registration(self):
        """Test registration with global registry."""
        # Clean up first
        if "test_global" in enhanced_registry._source_connectors:
            del enhanced_registry._source_connectors["test_global"]

        enhanced_registry.register_source("test_global", MockSourceConnector)

        # Verify registration
        assert "test_global" in enhanced_registry.list_source_connectors()

        # Clean up
        del enhanced_registry._source_connectors["test_global"]


class TestIntegrationScenarios:
    """Integration test scenarios that combine multiple registry features."""

    @pytest.fixture
    def populated_registry(self):
        """Create a registry with multiple connectors for integration testing."""
        registry = EnhancedConnectorRegistry()

        # Register CSV-like connector
        registry.register_source(
            "csv",
            MockSourceConnector,
            default_config={"has_header": True, "delimiter": ",", "encoding": "utf-8"},
            required_params=["path"],
            description="CSV file connector",
        )

        # Register database-like connector
        registry.register_source(
            "postgres",
            MockSourceConnector,
            default_config={"port": 5432, "timeout": 30, "ssl": False},
            required_params=["host", "database", "username"],
            description="PostgreSQL database connector",
        )

        return registry

    def test_csv_profile_scenario(self, populated_registry):
        """Test a realistic CSV connector profile scenario."""
        # Simulate profile configuration
        profile_params = {
            "path": "/data/input.csv",
            "delimiter": "|",  # Override default
        }

        # Simulate OPTIONS override
        override_options = {
            "encoding": "latin1",  # Override profile/default
            "skip_rows": 1,  # New parameter
        }

        result = populated_registry.resolve_configuration(
            "csv",
            is_source=True,
            profile_params=profile_params,
            override_options=override_options,
        )

        expected_config = {
            "has_header": True,  # Default
            "delimiter": "|",  # Profile override
            "encoding": "latin1",  # OPTIONS override
            "path": "/data/input.csv",  # Profile
            "skip_rows": 1,  # OPTIONS only
        }

        assert result.resolved_config == expected_config
        assert result.profile_used is True
        assert "encoding" in result.overridden_params

        # Create connector with resolved config
        connector = populated_registry.create_source_connector(
            "csv", result.resolved_config
        )
        assert isinstance(connector, MockSourceConnector)

    def test_database_profile_scenario(self, populated_registry):
        """Test a realistic database connector profile scenario."""
        # Simulate profile configuration
        profile_params = {
            "host": "prod-db.example.com",
            "database": "analytics",
            "username": "readonly_user",
            "password": "secret123",
            "port": 5433,  # Override default
        }

        result = populated_registry.resolve_configuration(
            "postgres", is_source=True, profile_params=profile_params
        )

        expected_config = {
            "host": "prod-db.example.com",
            "database": "analytics",
            "username": "readonly_user",
            "password": "secret123",
            "port": 5433,  # Profile override
            "timeout": 30,  # Default
            "ssl": False,  # Default
        }

        assert result.resolved_config == expected_config
        assert result.profile_used is True
        assert len(result.validation_warnings) == 0  # All required params provided

    def test_missing_required_params_scenario(self, populated_registry):
        """Test scenario with missing required parameters."""
        # Incomplete profile (missing required params)
        profile_params = {
            "host": "localhost"
            # Missing: database, username
        }

        result = populated_registry.resolve_configuration(
            "postgres", is_source=True, profile_params=profile_params
        )

        # Should still resolve but with warnings
        assert "host" in result.resolved_config
        assert len(result.validation_warnings) >= 2  # Missing database and username
        assert any("database" in warning for warning in result.validation_warnings)
        assert any("username" in warning for warning in result.validation_warnings)
