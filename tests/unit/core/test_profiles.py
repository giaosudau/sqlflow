"""Unit tests for SQLFlow ProfileManager."""

import os
import shutil
import tempfile
import time
from unittest.mock import patch

import pytest
import yaml

from sqlflow.core.profiles import ConnectorProfile, ProfileManager, ValidationResult


class TestConnectorProfile:
    """Test ConnectorProfile class."""

    def test_from_dict_valid_config(self):
        """Test creating ConnectorProfile from valid configuration."""
        config = {"type": "csv", "params": {"has_header": True, "delimiter": ","}}

        profile = ConnectorProfile.from_dict("test_csv", config)

        assert profile.name == "test_csv"
        assert profile.connector_type == "csv"
        assert profile.params == {"has_header": True, "delimiter": ","}

    def test_from_dict_missing_type(self):
        """Test error when type field is missing."""
        config = {"params": {"path": "test.csv"}}

        with pytest.raises(ValueError, match="missing required 'type' field"):
            ConnectorProfile.from_dict("test_csv", config)

    def test_from_dict_invalid_config_type(self):
        """Test error when config is not a dictionary."""
        with pytest.raises(ValueError, match="configuration must be a dictionary"):
            ConnectorProfile.from_dict("test_csv", "invalid")

    def test_from_dict_invalid_params_type(self):
        """Test error when params is not a dictionary."""
        config = {"type": "csv", "params": "invalid"}

        with pytest.raises(ValueError, match="params must be a dictionary"):
            ConnectorProfile.from_dict("test_csv", config)

    def test_from_dict_missing_params(self):
        """Test default empty params when params field is missing."""
        config = {"type": "csv"}

        profile = ConnectorProfile.from_dict("test_csv", config)

        assert profile.params == {}


class TestValidationResult:
    """Test ValidationResult class."""

    def test_bool_valid(self):
        """Test ValidationResult boolean conversion for valid result."""
        result = ValidationResult(is_valid=True, errors=[], warnings=[])
        assert bool(result) is True

    def test_bool_invalid(self):
        """Test ValidationResult boolean conversion for invalid result."""
        result = ValidationResult(is_valid=False, errors=["error"], warnings=[])
        assert bool(result) is False


class TestProfileManager:
    """Test ProfileManager class."""

    @pytest.fixture
    def temp_profile_dir(self):
        """Create temporary directory for profile tests."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def sample_profile_data(self):
        """Sample valid profile data."""
        return {
            "version": "1.0",
            "variables": {"env": "test", "db_host": "localhost"},
            "connectors": {
                "csv_default": {
                    "type": "csv",
                    "params": {"has_header": True, "delimiter": ","},
                },
                "postgres_main": {
                    "type": "postgres",
                    "params": {
                        "host": "${db_host}",
                        "port": 5432,
                        "database": "test_db",
                    },
                },
            },
            "engines": {"duckdb": {"mode": "memory"}},
        }

    def test_init(self, temp_profile_dir):
        """Test ProfileManager initialization."""
        manager = ProfileManager(temp_profile_dir, "test")

        assert manager.profile_dir == temp_profile_dir
        assert manager.environment == "test"
        assert len(manager._profile_cache) == 0

    def test_load_profile_valid(self, temp_profile_dir, sample_profile_data):
        """Test loading valid profile."""
        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile_data, f)

        manager = ProfileManager(temp_profile_dir, "test")
        profile = manager.load_profile()

        # After variable substitution, ${db_host} should be replaced with 'localhost'
        expected_profile = sample_profile_data.copy()
        expected_profile["connectors"]["postgres_main"]["params"]["host"] = "localhost"

        assert profile == expected_profile
        assert len(manager._profile_cache) == 1

    def test_load_profile_missing_file(self, temp_profile_dir):
        """Test error when profile file doesn't exist."""
        manager = ProfileManager(temp_profile_dir, "missing")

        with pytest.raises(FileNotFoundError, match="Profile file not found"):
            manager.load_profile()

    def test_load_profile_invalid_yaml(self, temp_profile_dir):
        """Test error when profile YAML is invalid."""
        # Create invalid YAML file
        profile_path = os.path.join(temp_profile_dir, "invalid.yml")
        with open(profile_path, "w") as f:
            f.write("invalid: yaml: content: [")

        manager = ProfileManager(temp_profile_dir, "invalid")

        with pytest.raises(ValueError, match="Invalid YAML"):
            manager.load_profile()

    def test_load_profile_validation_error(self, temp_profile_dir):
        """Test error when profile validation fails."""
        # Create profile with invalid structure
        invalid_profile = {
            "connectors": {
                "bad_connector": {
                    # Missing 'type' field
                    "params": {"path": "test.csv"}
                }
            }
        }

        profile_path = os.path.join(temp_profile_dir, "invalid.yml")
        with open(profile_path, "w") as f:
            yaml.dump(invalid_profile, f)

        manager = ProfileManager(temp_profile_dir, "invalid")

        with pytest.raises(ValueError, match="Profile validation failed"):
            manager.load_profile()

    def test_load_profile_caching(self, temp_profile_dir, sample_profile_data):
        """Test profile caching behavior."""
        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile_data, f)

        manager = ProfileManager(temp_profile_dir, "test")

        # First load
        with patch("builtins.open", side_effect=open) as mock_open:
            profile1 = manager.load_profile()
            mock_open.call_count

        # Second load should use cache
        with patch("builtins.open", side_effect=open) as mock_open:
            profile2 = manager.load_profile()
            second_call_count = mock_open.call_count

        assert profile1 == profile2
        # Second load should not open file (cached)
        assert second_call_count == 0

    def test_load_profile_cache_invalidation(
        self, temp_profile_dir, sample_profile_data
    ):
        """Test cache invalidation when file is modified."""
        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile_data, f)

        manager = ProfileManager(temp_profile_dir, "test")

        # First load
        profile1 = manager.load_profile()

        # Modify file (simulate by updating mtime)
        time.sleep(0.1)  # Ensure different timestamp
        os.utime(profile_path)

        # Second load should reload from file
        with patch("builtins.open", side_effect=open) as mock_open:
            profile2 = manager.load_profile()
            # Should have opened file again
            assert mock_open.call_count > 0

        assert profile1 == profile2  # Content same, but reloaded

    def test_load_profile_different_environments(self, temp_profile_dir):
        """Test loading different environment profiles."""
        # Create dev profile
        dev_profile = {"variables": {"env": "dev"}}
        with open(os.path.join(temp_profile_dir, "dev.yml"), "w") as f:
            yaml.dump(dev_profile, f)

        # Create prod profile
        prod_profile = {"variables": {"env": "prod"}}
        with open(os.path.join(temp_profile_dir, "prod.yml"), "w") as f:
            yaml.dump(prod_profile, f)

        manager = ProfileManager(temp_profile_dir, "dev")

        dev_data = manager.load_profile("dev")
        prod_data = manager.load_profile("prod")

        assert dev_data["variables"]["env"] == "dev"
        assert prod_data["variables"]["env"] == "prod"
        assert len(manager._profile_cache) == 2  # Both cached

    def test_get_connector_profile_valid(self, temp_profile_dir, sample_profile_data):
        """Test getting valid connector profile."""
        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile_data, f)

        manager = ProfileManager(temp_profile_dir, "test")
        connector = manager.get_connector_profile("csv_default")

        assert connector.name == "csv_default"
        assert connector.connector_type == "csv"
        assert connector.params == {"has_header": True, "delimiter": ","}

    def test_get_connector_profile_not_found(
        self, temp_profile_dir, sample_profile_data
    ):
        """Test error when connector not found."""
        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile_data, f)

        manager = ProfileManager(temp_profile_dir, "test")

        with pytest.raises(ValueError, match="Connector 'missing' not found"):
            manager.get_connector_profile("missing")

    def test_validate_profile_valid(self, temp_profile_dir, sample_profile_data):
        """Test validation of valid profile."""
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        manager = ProfileManager(temp_profile_dir, "test")

        result = manager.validate_profile(profile_path, sample_profile_data)

        assert result.is_valid
        assert len(result.errors) == 0

    def test_validate_profile_invalid_structure(self, temp_profile_dir):
        """Test validation of profile with invalid structure."""
        invalid_profile = "not a dictionary"
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        manager = ProfileManager(temp_profile_dir, "test")

        result = manager.validate_profile(profile_path, invalid_profile)

        assert not result.is_valid
        assert "Profile must be a YAML dictionary" in result.errors

    def test_validate_profile_invalid_connectors(self, temp_profile_dir):
        """Test validation of profile with invalid connectors."""
        invalid_profile = {
            "connectors": {
                "bad_connector": {
                    # Missing 'type' field
                    "params": {"path": "test.csv"}
                }
            }
        }
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        manager = ProfileManager(temp_profile_dir, "test")

        result = manager.validate_profile(profile_path, invalid_profile)

        assert not result.is_valid
        assert any("missing required 'type' field" in error for error in result.errors)

    def test_validate_profile_invalid_duckdb_mode(self, temp_profile_dir):
        """Test validation of profile with invalid DuckDB mode."""
        invalid_profile = {"engines": {"duckdb": {"mode": "invalid_mode"}}}
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        manager = ProfileManager(temp_profile_dir, "test")

        result = manager.validate_profile(profile_path, invalid_profile)

        assert not result.is_valid
        assert any(
            "Invalid DuckDB mode 'invalid_mode'" in error for error in result.errors
        )

    def test_validate_profile_warnings(self, temp_profile_dir):
        """Test validation warnings for unknown version."""
        profile_with_warnings = {"version": "2.0", "connectors": {}}  # Unknown version
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        manager = ProfileManager(temp_profile_dir, "test")

        result = manager.validate_profile(profile_path, profile_with_warnings)

        assert result.is_valid  # Still valid despite warning
        assert any(
            "Unknown profile version '2.0'" in warning for warning in result.warnings
        )

    def test_list_connectors(self, temp_profile_dir, sample_profile_data):
        """Test listing available connectors."""
        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile_data, f)

        manager = ProfileManager(temp_profile_dir, "test")
        connectors = manager.list_connectors()

        assert set(connectors) == {"csv_default", "postgres_main"}

    def test_list_connectors_missing_profile(self, temp_profile_dir):
        """Test listing connectors when profile doesn't exist."""
        manager = ProfileManager(temp_profile_dir, "missing")
        connectors = manager.list_connectors()

        assert connectors == []

    def test_get_variables(self, temp_profile_dir, sample_profile_data):
        """Test getting profile variables."""
        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile_data, f)

        manager = ProfileManager(temp_profile_dir, "test")
        variables = manager.get_variables()

        assert variables == {"env": "test", "db_host": "localhost"}

    def test_get_variables_missing_profile(self, temp_profile_dir):
        """Test getting variables when profile doesn't exist."""
        manager = ProfileManager(temp_profile_dir, "missing")
        variables = manager.get_variables()

        assert variables == {}

    def test_clear_cache(self, temp_profile_dir, sample_profile_data):
        """Test clearing profile cache."""
        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile_data, f)

        manager = ProfileManager(temp_profile_dir, "test")

        # Load profile to populate cache
        manager.load_profile()
        assert len(manager._profile_cache) == 1

        # Clear cache
        manager.clear_cache()
        assert len(manager._profile_cache) == 0
        assert len(manager._cache_timestamps) == 0

    def test_get_cache_stats(self, temp_profile_dir, sample_profile_data):
        """Test getting cache statistics."""
        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile_data, f)

        manager = ProfileManager(temp_profile_dir, "test")

        # Initial stats
        stats = manager.get_cache_stats()
        assert stats["cached_profiles"] == 0

        # Load profile and check stats
        manager.load_profile()
        stats = manager.get_cache_stats()
        assert stats["cached_profiles"] == 1
        assert len(stats["cache_keys"]) == 1
        assert stats["cache_size_bytes"] > 0

    def test_performance_profile_loading(self, temp_profile_dir, sample_profile_data):
        """Test profile loading performance (should be < 50ms)."""
        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile_data, f)

        manager = ProfileManager(temp_profile_dir, "test")

        # Measure loading time
        start_time = time.time()
        manager.load_profile()
        load_time = (time.time() - start_time) * 1000  # Convert to ms

        # Should be less than 50ms (requirement from DOD)
        assert (
            load_time < 50
        ), f"Profile loading took {load_time:.2f}ms, should be < 50ms"

    def test_memory_usage_no_leaks(self, temp_profile_dir, sample_profile_data):
        """Test memory usage and check for leaks in repeated operations."""
        import gc

        # Create profile file
        profile_path = os.path.join(temp_profile_dir, "test.yml")
        with open(profile_path, "w") as f:
            yaml.dump(sample_profile_data, f)

        manager = ProfileManager(temp_profile_dir, "test")

        # Get baseline memory usage
        gc.collect()
        initial_objects = len(gc.get_objects())

        # Perform many operations
        for _ in range(100):
            manager.load_profile()
            manager.get_connector_profile("csv_default")
            manager.list_connectors()
            manager.get_variables()
            manager.clear_cache()

        # Check for memory leaks
        gc.collect()
        final_objects = len(gc.get_objects())

        # Allow for some variance, but shouldn't grow significantly
        object_growth = final_objects - initial_objects
        assert (
            object_growth < 100
        ), f"Potential memory leak: {object_growth} new objects created"
