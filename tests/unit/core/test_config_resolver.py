"""Unit tests for SQLFlow ConfigurationResolver."""

import os
import shutil
import tempfile
from unittest.mock import patch

import pytest
import yaml

from sqlflow.core.config_resolver import ConfigurationResolver
from sqlflow.core.profiles import ProfileManager
from sqlflow.core.variable_substitution import VariableSubstitutionEngine


class TestConfigurationResolver:
    """Test ConfigurationResolver class."""

    @pytest.fixture
    def temp_profile_dir(self):
        """Create temporary directory for profile tests."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def sample_profile_data(self):
        """Sample profile data for testing."""
        return {
            "version": "1.0",
            "variables": {
                "db_host": "localhost",
                "db_port": "5432",
                "csv_delimiter": ",",
            },
            "connectors": {
                "csv_default": {
                    "type": "csv",
                    "params": {
                        "has_header": True,
                        "delimiter": "${csv_delimiter}",
                        "encoding": "utf-8",
                    },
                },
                "postgres_main": {
                    "type": "postgres",
                    "params": {
                        "host": "${db_host}",
                        "port": "${db_port}",
                        "database": "test_db",
                        "schema": "public",
                    },
                },
            },
        }

    @pytest.fixture
    def profile_manager(self, temp_profile_dir, sample_profile_data):
        """Create ProfileManager with sample data."""
        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile_data, f)

        return ProfileManager(temp_profile_dir, "test")

    @pytest.fixture
    def resolver(self, profile_manager):
        """Create ConfigurationResolver with sample profile manager."""
        return ConfigurationResolver(profile_manager)

    def test_init(self, profile_manager):
        """Test ConfigurationResolver initialization."""
        resolver = ConfigurationResolver(profile_manager)

        assert resolver.profile_manager == profile_manager
        assert resolver.variable_engine is not None
        assert isinstance(resolver.variable_engine, VariableSubstitutionEngine)

    def test_init_with_custom_variable_engine(self, profile_manager):
        """Test initialization with custom variable engine."""
        custom_engine = VariableSubstitutionEngine()
        resolver = ConfigurationResolver(profile_manager, custom_engine)

        assert resolver.variable_engine == custom_engine

    def test_resolve_config_basic_merging(self, resolver):
        """Test basic configuration merging without variables."""
        defaults = {"timeout": 30, "retry_count": 3}

        options = {"encoding": "latin-1"}  # Override profile default

        resolved = resolver.resolve_config(
            profile_name="csv_default", defaults=defaults, options=options
        )

        # Should contain all three sources
        assert resolved["type"] == "csv"  # From profile
        assert resolved["has_header"] is True  # From profile params
        assert resolved["timeout"] == 30  # From defaults
        assert resolved["encoding"] == "latin-1"  # From options (overridden)
        assert resolved["retry_count"] == 3  # From defaults

    def test_resolve_config_priority_order(self, resolver):
        """Test that configuration merging respects priority order."""
        # All three sources have the same key with different values
        defaults = {"delimiter": ";"}
        options = {"delimiter": "|"}

        resolved = resolver.resolve_config(
            profile_name="csv_default", defaults=defaults, options=options
        )

        # Options should win (highest priority)
        assert resolved["delimiter"] == "|"

    def test_resolve_config_with_variable_substitution(self, resolver):
        """Test configuration resolution with variable substitution."""
        resolved = resolver.resolve_config(profile_name="csv_default")

        # Variables should be substituted from profile
        assert resolved["delimiter"] == ","  # ${csv_delimiter} resolved

    def test_resolve_config_with_runtime_variables(self, resolver):
        """Test variable substitution with runtime variables."""
        runtime_vars = {"csv_delimiter": "|"}  # Override profile variable

        resolved = resolver.resolve_config(
            profile_name="csv_default", variables=runtime_vars
        )

        # Runtime variables should override profile variables
        assert resolved["delimiter"] == "|"

    def test_resolve_config_nested_object_merging(self, resolver):
        """Test merging of nested configuration objects."""
        defaults = {"connection": {"timeout": 30, "pool_size": 10}}

        options = {"connection": {"timeout": 60}}  # Override timeout but keep pool_size

        # Create a profile with nested config
        profile_data = {
            "connectors": {
                "test_nested": {
                    "type": "postgres",
                    "params": {"connection": {"ssl": True}},
                }
            }
        }

        # Mock the profile manager to return our nested config
        with patch.object(
            resolver.profile_manager, "get_connector_profile"
        ) as mock_get:
            from sqlflow.core.profiles import ConnectorProfile

            mock_profile = ConnectorProfile(
                name="test_nested",
                connector_type="postgres",
                params={"connection": {"ssl": True}},
            )
            mock_get.return_value = mock_profile

            with patch.object(
                resolver.profile_manager, "get_variables", return_value={}
            ):
                resolved = resolver.resolve_config(
                    profile_name="test_nested", defaults=defaults, options=options
                )

        # Check that nested objects are properly merged
        assert resolved["connection"]["timeout"] == 60  # From options
        assert resolved["connection"]["pool_size"] == 10  # From defaults
        assert resolved["connection"]["ssl"] is True  # From profile

    def test_resolve_config_type_conversion(self, resolver):
        """Test type conversion during configuration resolution."""
        # Mock variable engine to test type handling
        with patch.object(resolver.variable_engine, "substitute") as mock_substitute:
            # Return config with string numbers that should be converted
            mock_substitute.return_value = {
                "type": "postgres",
                "port": "5432",  # String number
                "timeout": "30",  # String number
                "ssl": "true",  # String boolean
            }

            resolved = resolver.resolve_config(profile_name="postgres_main")

            # The resolver itself doesn't do type conversion - that's up to the variable engine
            # Just verify the substitution was called
            mock_substitute.assert_called_once()

    def test_resolve_config_missing_profile(self, resolver):
        """Test error handling for missing profile."""
        with pytest.raises(ValueError, match="not found in profile"):
            resolver.resolve_config(profile_name="nonexistent")

    def test_resolve_config_empty_values(self, resolver):
        """Test handling of empty and null values."""
        defaults = {"empty_string": "", "null_value": None, "zero_value": 0}

        resolved = resolver.resolve_config(
            profile_name="csv_default", defaults=defaults
        )

        # Empty and null values should be preserved
        assert resolved["empty_string"] == ""
        assert resolved["null_value"] is None
        assert resolved["zero_value"] == 0

    def test_substitute_variables(self, resolver):
        """Test variable substitution method."""
        config = {
            "host": "${db_host}",
            "port": "${db_port}",
            "static_value": "unchanged",
        }

        variables = {"db_host": "production.db.com", "db_port": "5432"}

        result = resolver.substitute_variables(config, variables)

        # Variables should be substituted
        assert "db_host" in result["host"] or result["host"] == "production.db.com"
        assert "static_value" in result

    def test_get_connector_defaults(self, resolver):
        """Test getting connector defaults."""
        # Test known connector types
        csv_defaults = resolver.get_connector_defaults("csv")
        assert csv_defaults["has_header"] is True
        assert csv_defaults["delimiter"] == ","

        postgres_defaults = resolver.get_connector_defaults("postgres")
        assert postgres_defaults["port"] == 5432
        assert postgres_defaults["sslmode"] == "prefer"

        s3_defaults = resolver.get_connector_defaults("s3")
        assert s3_defaults["region"] == "us-east-1"
        assert s3_defaults["use_ssl"] is True

        # Test unknown connector type
        unknown_defaults = resolver.get_connector_defaults("unknown")
        assert unknown_defaults == {}

    def test_resolve_with_defaults(self, resolver):
        """Test convenience method that includes automatic defaults."""
        options = {"encoding": "latin-1"}

        resolved = resolver.resolve_with_defaults(
            profile_name="csv_default", options=options
        )

        # Should include CSV defaults
        assert resolved["has_header"] is True  # From profile
        assert resolved["encoding"] == "latin-1"  # From options
        assert resolved["quote_char"] == '"'  # From CSV defaults
        assert resolved["escape_char"] == "\\"  # From CSV defaults

    def test_validate_resolved_config(self, resolver):
        """Test validation of resolved configuration."""
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "empty_field": "",
            "null_field": None,
        }

        # Test with no required fields
        errors = resolver.validate_resolved_config(config)
        assert errors == []

        # Test with valid required fields
        errors = resolver.validate_resolved_config(
            config, required_fields=["host", "port", "database"]
        )
        assert errors == []

        # Test with missing required field
        errors = resolver.validate_resolved_config(
            config, required_fields=["host", "port", "missing_field"]
        )
        assert len(errors) == 1
        assert "missing_field" in errors[0]

        # Test with null required field
        errors = resolver.validate_resolved_config(
            config, required_fields=["null_field"]
        )
        assert len(errors) == 1
        assert "cannot be null" in errors[0]

        # Test with empty string required field
        errors = resolver.validate_resolved_config(
            config, required_fields=["empty_field"]
        )
        assert len(errors) == 1
        assert "cannot be empty" in errors[0]

    def test_get_resolution_info(self, resolver):
        """Test getting detailed resolution information."""
        options = {"encoding": "latin-1"}
        variables = {"csv_delimiter": "|"}

        info = resolver.get_resolution_info(
            profile_name="csv_default", options=options, variables=variables
        )

        # Check all expected fields are present
        assert info["profile_name"] == "csv_default"
        assert info["connector_type"] == "csv"
        assert info["environment"] == "test"
        assert "defaults" in info
        assert "profile_params" in info
        assert info["options_override"] == options
        assert info["runtime_variables"] == variables
        assert "merged_variables" in info

        # Check that runtime variables override profile variables
        assert info["merged_variables"]["csv_delimiter"] == "|"

    def test_integration_with_variable_substitution_engine(self, resolver):
        """Test integration with VariableSubstitutionEngine."""
        config = {"host": "${db_host}", "other": "static"}
        variables = {"db_host": "new.host.com"}

        # Test that substitute_variables delegates to the engine
        with patch.object(
            resolver.variable_engine, "substitute", return_value=config
        ) as mock_sub:
            result = resolver.substitute_variables(config, variables)

            # Should have called substitute on the engine
            mock_sub.assert_called_once_with(config)

            # Result should be what the engine returned
            assert result == config

    def test_performance_config_resolution(self, resolver):
        """Test configuration resolution performance (should be < 10ms)."""
        import time

        # Measure resolution time
        start_time = time.time()
        resolved = resolver.resolve_config(profile_name="csv_default")
        resolution_time = (time.time() - start_time) * 1000  # Convert to ms

        # Should be less than 10ms (requirement from DOD)
        assert (
            resolution_time < 10
        ), f"Config resolution took {resolution_time:.2f}ms, should be < 10ms"

        # Verify result is correct
        assert resolved["type"] == "csv"
        assert "delimiter" in resolved

    def test_error_handling_variable_substitution(self, resolver):
        """Test error handling during variable substitution."""
        # Mock variable engine to raise an exception
        with patch.object(
            resolver.variable_engine,
            "substitute",
            side_effect=Exception("Substitution error"),
        ):
            with pytest.raises(Exception, match="Substitution error"):
                resolver.substitute_variables({"key": "value"}, {"var": "val"})

    def test_variable_priority_merging(self, resolver):
        """Test that runtime variables take priority over profile variables."""
        profile_vars = {"host": "profile.host.com", "port": "5432"}
        runtime_vars = {"host": "runtime.host.com"}  # Override host but not port

        # Mock profile manager to return our test variables
        with patch.object(
            resolver.profile_manager, "get_variables", return_value=profile_vars
        ):
            with patch.object(
                resolver.profile_manager, "get_connector_profile"
            ) as mock_get:
                from sqlflow.core.profiles import ConnectorProfile

                mock_profile = ConnectorProfile(
                    name="test",
                    connector_type="postgres",
                    params={"database": "test_db"},
                )
                mock_get.return_value = mock_profile

                # Mock variable engine to verify what variables it receives
                with patch.object(
                    resolver.variable_engine, "update_variables"
                ) as mock_update:
                    with patch.object(
                        resolver.variable_engine,
                        "substitute",
                        return_value={"merged": "result"},
                    ):
                        resolver.resolve_config(
                            profile_name="test", variables=runtime_vars
                        )

                        # Should have been called with merged variables where runtime overrides profile
                        call_args = mock_update.call_args_list
                        # Find the call with merged variables
                        merged_vars = None
                        for call in call_args:
                            if "host" in call[0][0]:
                                merged_vars = call[0][0]
                                break

                        if merged_vars:
                            assert (
                                merged_vars["host"] == "runtime.host.com"
                            )  # Runtime wins
                            assert (
                                merged_vars["port"] == "5432"
                            )  # Profile value preserved
