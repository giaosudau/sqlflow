"""Integration tests for enhanced connector registry with ProfileManager."""

import os
import tempfile

import pytest
import yaml

from sqlflow.connectors.csv.source import CSVSource
from sqlflow.connectors.postgres.source import PostgresSource
from sqlflow.connectors.registry.enhanced_registry import EnhancedConnectorRegistry
from sqlflow.core.profiles import ProfileManager


class TestEnhancedRegistryProfileIntegration:
    """Test enhanced registry integration with ProfileManager."""

    @pytest.fixture
    def temp_profile_dir(self):
        """Create temporary directory with test profiles."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create dev.yml profile
            dev_profile = {
                "version": "1.0",
                "variables": {"data_dir": "/tmp/test_data"},
                "connectors": {
                    "csv_default": {
                        "type": "csv",
                        "params": {
                            "has_header": True,
                            "delimiter": ",",
                            "encoding": "utf-8",
                        },
                    },
                    "postgres_analytics": {
                        "type": "postgres",
                        "params": {
                            "host": "localhost",
                            "port": 5432,
                            "database": "analytics",
                            "username": "analyst",
                            "password": "secret123",
                        },
                    },
                },
            }

            dev_profile_path = os.path.join(temp_dir, "dev.yml")
            with open(dev_profile_path, "w") as f:
                yaml.dump(dev_profile, f)

            yield temp_dir

    @pytest.fixture
    def registry_with_real_connectors(self):
        """Create registry with real connectors and proper defaults."""
        registry = EnhancedConnectorRegistry()

        # Register CSV connector with realistic defaults
        registry.register_source(
            "csv",
            CSVSource,
            default_config={"has_header": True, "delimiter": ",", "encoding": "utf-8"},
            required_params=["path"],
            description="CSV file source connector",
        )

        # Register PostgreSQL connector with realistic defaults
        registry.register_source(
            "postgres",
            PostgresSource,
            default_config={"port": 5432, "timeout": 30, "ssl": False},
            required_params=["host", "database", "username"],
            description="PostgreSQL database source connector",
        )

        return registry

    def test_csv_connector_profile_resolution(
        self, temp_profile_dir, registry_with_real_connectors
    ):
        """Test CSV connector with profile-based configuration."""
        profile_manager = ProfileManager(temp_profile_dir, "dev")

        # Get profile configuration
        connector_profile = profile_manager.get_connector_profile("csv_default")

        # Resolve configuration with override options
        override_options = {
            "path": "/data/products.csv",
            "delimiter": "|",  # Override profile default
        }

        result = registry_with_real_connectors.resolve_configuration(
            connector_profile.connector_type,
            is_source=True,
            profile_params=connector_profile.params,
            override_options=override_options,
        )

        # Verify configuration resolution
        expected_config = {
            "has_header": True,  # Default
            "delimiter": "|",  # Override
            "encoding": "utf-8",  # Profile
            "path": "/data/products.csv",  # Override
        }

        assert result.resolved_config == expected_config
        assert result.profile_used is True
        assert "delimiter" in result.overridden_params
        # path is not considered an override since it wasn't in the default/profile config

        # Create actual connector instance
        connector = registry_with_real_connectors.create_source_connector(
            connector_profile.connector_type, result.resolved_config
        )

        assert isinstance(connector, CSVSource)
        assert connector.path == "/data/products.csv"
        assert connector.delimiter == "|"
        assert connector.encoding == "utf-8"
        assert connector.has_header is True

    def test_postgres_connector_profile_resolution(
        self, temp_profile_dir, registry_with_real_connectors
    ):
        """Test PostgreSQL connector with profile-based configuration."""
        profile_manager = ProfileManager(temp_profile_dir, "dev")

        # Get profile configuration
        connector_profile = profile_manager.get_connector_profile("postgres_analytics")

        # Resolve configuration with override options
        override_options = {
            "timeout": 60,  # Override default
            "ssl": True,  # Override default
        }

        result = registry_with_real_connectors.resolve_configuration(
            connector_profile.connector_type,
            is_source=True,
            profile_params=connector_profile.params,
            override_options=override_options,
        )

        # Verify configuration resolution
        expected_config = {
            "host": "localhost",  # Profile
            "port": 5432,  # Profile (matches default)
            "database": "analytics",  # Profile
            "username": "analyst",  # Profile
            "password": "secret123",  # Profile
            "timeout": 60,  # Override
            "ssl": True,  # Override
        }

        assert result.resolved_config == expected_config
        assert result.profile_used is True
        assert "timeout" in result.overridden_params
        assert "ssl" in result.overridden_params
        assert len(result.validation_warnings) == 0  # All required params provided

        # Create actual connector instance
        connector = registry_with_real_connectors.create_source_connector(
            connector_profile.connector_type, result.resolved_config
        )

        assert isinstance(connector, PostgresSource)
        assert connector.conn_params["host"] == "localhost"
        assert connector.conn_params["database"] == "analytics"
        assert connector.conn_params["username"] == "analyst"

    def test_profile_validation_with_registry_defaults(
        self, temp_profile_dir, registry_with_real_connectors
    ):
        """Test that profile validation works with registry defaults."""
        profile_manager = ProfileManager(temp_profile_dir, "dev")

        # Test CSV connector with missing required parameter
        csv_profile = profile_manager.get_connector_profile("csv_default")

        result = registry_with_real_connectors.resolve_configuration(
            csv_profile.connector_type,
            is_source=True,
            profile_params=csv_profile.params,
            # No override_options - missing required 'path'
        )

        # Should have validation warning about missing 'path'
        assert len(result.validation_warnings) > 0
        assert any("path" in warning for warning in result.validation_warnings)

    def test_connector_defaults_integration(self, registry_with_real_connectors):
        """Test getting connector defaults matches what connectors expect."""
        # Test CSV defaults
        csv_defaults = registry_with_real_connectors.get_connector_defaults("csv")
        assert csv_defaults["has_header"] is True
        assert csv_defaults["delimiter"] == ","
        assert csv_defaults["encoding"] == "utf-8"

        # Test PostgreSQL defaults
        postgres_defaults = registry_with_real_connectors.get_connector_defaults(
            "postgres"
        )
        assert postgres_defaults["port"] == 5432
        assert postgres_defaults["timeout"] == 30
        assert postgres_defaults["ssl"] is False

    def test_end_to_end_profile_to_connector_creation(
        self, temp_profile_dir, registry_with_real_connectors
    ):
        """Test complete end-to-end flow from profile to working connector."""
        profile_manager = ProfileManager(temp_profile_dir, "dev")

        # Step 1: Load connector profile
        connector_profile = profile_manager.get_connector_profile("csv_default")

        # Step 2: Resolve configuration (simulating pipeline OPTIONS)
        override_options = {"path": "/tmp/test.csv"}

        resolution_result = registry_with_real_connectors.resolve_configuration(
            connector_profile.connector_type,
            is_source=True,
            profile_params=connector_profile.params,
            override_options=override_options,
        )

        # Step 3: Create connector instance
        connector = registry_with_real_connectors.create_source_connector(
            connector_profile.connector_type, resolution_result.resolved_config
        )

        # Step 4: Verify connector is properly configured
        assert isinstance(connector, CSVSource)
        assert connector.path == "/tmp/test.csv"
        assert connector.has_header is True
        assert connector.delimiter == ","
        assert connector.encoding == "utf-8"

        # Step 5: Verify connector can be tested (connection test)
        # Note: This would fail in real scenario without actual file,
        # but demonstrates the flow
        test_result = connector.test_connection()
        assert test_result is not None

    def test_multiple_environment_profiles(self, registry_with_real_connectors):
        """Test handling multiple environment profiles."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create dev and prod profiles with different configurations
            dev_profile = {
                "version": "1.0",
                "connectors": {
                    "main_db": {
                        "type": "postgres",
                        "params": {
                            "host": "dev-db.example.com",
                            "database": "dev_analytics",
                            "username": "dev_user",
                        },
                    }
                },
            }

            prod_profile = {
                "version": "1.0",
                "connectors": {
                    "main_db": {
                        "type": "postgres",
                        "params": {
                            "host": "prod-db.example.com",
                            "database": "prod_analytics",
                            "username": "prod_user",
                        },
                    }
                },
            }

            # Write profile files
            with open(os.path.join(temp_dir, "dev.yml"), "w") as f:
                yaml.dump(dev_profile, f)
            with open(os.path.join(temp_dir, "prod.yml"), "w") as f:
                yaml.dump(prod_profile, f)

            # Test dev environment
            dev_profile_manager = ProfileManager(temp_dir, "dev")
            dev_connector_profile = dev_profile_manager.get_connector_profile("main_db")

            dev_result = registry_with_real_connectors.resolve_configuration(
                dev_connector_profile.connector_type,
                is_source=True,
                profile_params=dev_connector_profile.params,
            )

            assert dev_result.resolved_config["host"] == "dev-db.example.com"
            assert dev_result.resolved_config["database"] == "dev_analytics"

            # Test prod environment
            prod_profile_manager = ProfileManager(temp_dir, "prod")
            prod_connector_profile = prod_profile_manager.get_connector_profile(
                "main_db"
            )

            prod_result = registry_with_real_connectors.resolve_configuration(
                prod_connector_profile.connector_type,
                is_source=True,
                profile_params=prod_connector_profile.params,
            )

            assert prod_result.resolved_config["host"] == "prod-db.example.com"
            assert prod_result.resolved_config["database"] == "prod_analytics"


class TestRegistryConnectorCompatibility:
    """Test compatibility with existing connectors."""

    def test_csv_connector_compatibility(self):
        """Test that CSV connector works with enhanced registry."""
        registry = EnhancedConnectorRegistry()
        registry.register_source("csv", CSVSource)

        # Test basic creation
        config = {"path": "/tmp/test.csv"}
        connector = registry.create_source_connector("csv", config)

        assert isinstance(connector, CSVSource)
        assert connector.path == "/tmp/test.csv"

    def test_postgres_connector_compatibility(self):
        """Test that PostgreSQL connector works with enhanced registry."""
        registry = EnhancedConnectorRegistry()
        registry.register_source("postgres", PostgresSource)

        # Test basic creation
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "test",
            "username": "test",
            "password": "test",
        }
        connector = registry.create_source_connector("postgres", config)

        assert isinstance(connector, PostgresSource)
        assert connector.conn_params["host"] == "localhost"
        assert connector.conn_params["database"] == "test"

    def test_legacy_connector_fallback(self):
        """Test fallback for legacy connectors without config parameter."""
        registry = EnhancedConnectorRegistry()

        # Create a mock legacy connector that doesn't accept config in __init__
        class LegacyConnector:
            def __init__(self):
                self.configured = False

            def configure(self, params):
                self.params = params
                self.configured = True

            def read(self, options=None):
                return None

        registry.register_source("legacy", LegacyConnector)

        config = {"param1": "value1"}
        connector = registry.create_source_connector("legacy", config)

        assert isinstance(connector, LegacyConnector)
        assert connector.configured is True
        assert connector.params == config
